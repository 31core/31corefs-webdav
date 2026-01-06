use actix_web::{App, HttpServer, web};
use clap::Parser;
use dav_server::{
    DavConfig, DavHandler,
    actix::{DavRequest, DavResponse},
    davpath::DavPath,
    fs::{
        DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult, FsStream,
        OpenOptions, ReadDirMeta,
    },
};
use fs31core::{Directory, File, Filesystem, Subvolume, block::BLOCK_SIZE};
use futures_util::FutureExt;
use std::{
    fs::File as FsFile,
    io::{Error, ErrorKind, Result as IOResult},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(about = "31coreFS WebDAV interface", version)]
struct Args {
    device: String,
    /** Listen address */
    #[arg(long, default_value = "localhost")]
    host: String,
    /** Listen port */
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
    /** Default subvolume */
    #[arg(short, long)]
    subvolume: Option<u64>,
}

#[derive(Debug)]
struct CoreFile {
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<FsFile>>,
    fd: File,
    offset: u64,
    metadata: CoreMetaData,
}

impl CoreFile {
    fn new(
        fs: Arc<Mutex<Filesystem>>,
        subvol: Arc<Mutex<Subvolume>>,
        device: Arc<Mutex<FsFile>>,
        fd: File,
        metadata: CoreMetaData,
    ) -> Self {
        Self {
            fs,
            subvol,
            device,
            fd,
            metadata,
            offset: 0,
        }
    }
}

impl DavFile for CoreFile {
    fn metadata(&'_ mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let metadata: Box<dyn DavMetaData> = Box::new(self.metadata.clone());
        async move { Ok(metadata) }.boxed()
    }
    fn write_buf(&'_ mut self, mut buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        async move {
            let mut bytes = vec![0; buf.remaining()];
            buf.copy_to_slice(&mut bytes);
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            self.fd.write(fs, subvol, device, self.offset, &bytes)?;
            fs.sync_meta_data(device)?;

            self.offset += bytes.len() as u64;
            self.metadata.size = self.fd.get_inode().size;

            Ok(())
        }
        .boxed()
    }

    fn write_bytes(&mut self, buf: bytes::Bytes) -> FsFuture<'_, ()> {
        async move {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            self.fd.write(fs, subvol, device, self.offset, &buf)?;
            fs.sync_meta_data(device)?;

            self.offset += buf.len() as u64;
            self.metadata.size = self.fd.get_inode().size;

            Ok(())
        }
        .boxed()
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, bytes::Bytes> {
        async move {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            let mut bytes = vec![0; count];
            self.fd
                .read(fs, subvol, device, self.offset, &mut bytes, count as u64)?;

            self.offset += count as u64;

            Ok(bytes::Bytes::from(bytes))
        }
        .boxed()
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        match pos {
            SeekFrom::Start(offset) => self.offset = offset,
            SeekFrom::Current(offset) => {
                if offset > 0 {
                    self.offset += offset as u64;
                } else {
                    self.offset -= -offset as u64;
                }
            }
            SeekFrom::End(offset) => {
                if offset > 0 {
                    self.offset = self.metadata.size + offset as u64;
                } else {
                    self.offset = self.metadata.size - -offset as u64;
                }
            }
        }
        async move { Ok(self.offset) }.boxed()
    }
    fn flush(&mut self) -> FsFuture<'_, ()> {
        self.offset = 0;
        async { Ok(()) }.boxed()
    }
}

#[derive(Debug, Clone)]
struct CoreFilesystem {
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<FsFile>>,
}

impl CoreFilesystem {
    fn new<P>(path: P, subvol_id: Option<u64>) -> anyhow::Result<Box<Self>>
    where
        P: AsRef<Path>,
    {
        let mut device = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let fs = Filesystem::load(&mut device)?;
        let subvol = match subvol_id {
            Some(id) => fs.get_subvolume(&mut device, id)?,
            None => fs.get_default_subvolume(&mut device)?,
        };

        let device = Arc::new(Mutex::new(device));
        let fs = Arc::new(Mutex::new(fs));
        let subvol = Arc::new(Mutex::new(subvol));

        handle_quit(Arc::clone(&fs), Arc::clone(&subvol), Arc::clone(&device))?;

        Ok(Box::new(Self { fs, device, subvol }))
    }
    async fn create_file<D>(
        &self,
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: &DavPath,
    ) -> FsResult<CoreFile>
    where
        D: Read + Write + Seek,
    {
        let fd = File::create(fs, subvol, device, path.as_pathbuf())?;

        fs.sync_meta_data(device)?;
        subvol.sync_meta_data(fs, device)?;

        let metadata = CoreMetaData::new(fs, subvol, device, path.as_pathbuf())?;
        Ok(CoreFile::new(
            Arc::clone(&self.fs),
            Arc::clone(&self.subvol),
            Arc::clone(&self.device),
            fd,
            metadata,
        ))
    }
}

impl DavFileSystem for CoreFilesystem {
    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        async move {
            let mut device = self.device.lock().await;
            let mut fs = self.fs.lock().await;
            let mut subvol = self.subvol.lock().await;

            if options.create_new {
                let file: Box<dyn DavFile> = Box::new(
                    self.create_file(&mut fs, &mut subvol, &mut device as &mut FsFile, path)
                        .await?,
                );
                return Ok(file);
            }

            match CoreMetaData::new(
                &mut fs,
                &mut subvol,
                &mut device as &mut FsFile,
                path.as_pathbuf(),
            ) {
                Ok(metadata) => Ok(Box::new(CoreFile::new(
                    Arc::clone(&self.fs),
                    Arc::clone(&self.subvol),
                    Arc::clone(&self.device),
                    File::open(
                        &mut fs,
                        &mut subvol,
                        &mut device as &mut FsFile,
                        path.as_pathbuf(),
                    )?,
                    metadata,
                )) as Box<dyn DavFile>),
                Err(_) => {
                    /* file does not exist, then create it */
                    if options.create {
                        let file: Box<dyn DavFile> = Box::new(
                            self.create_file(
                                &mut fs,
                                &mut subvol,
                                &mut device as &mut FsFile,
                                path,
                            )
                            .await?,
                        );
                        Ok(file)
                    } else {
                        Err(FsError::NotFound)
                    }
                }
            }
        }
        .boxed()
    }
    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await as &mut Filesystem;
            let subvol = &mut self.subvol.lock().await;

            let mut v: Vec<Result<Box<dyn DavDirEntry>, FsError>> = Vec::new();
            for name in fs.list_dir(subvol, device, path.as_pathbuf())? {
                v.push(Ok(Box::new(CoreDirEntry::new(
                    &name,
                    CoreMetaData::new(fs, subvol, device, &(path.to_string() + "/" + &name))?,
                ))));
            }
            let stream = futures_util::stream::iter(v);

            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            match CoreMetaData::new(fs, subvol, device, path.as_pathbuf()) {
                Ok(metadata) => {
                    let metadata: Box<dyn DavMetaData> = Box::new(metadata);
                    Ok(metadata)
                }
                Err(_) => Err(FsError::NotFound),
            }
        }
        .boxed()
    }
    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            fs.mkdir(subvol, device, path.as_pathbuf())?;
            fs.sync_meta_data(device)?;
            subvol.sync_meta_data(fs, device)?;

            Ok(())
        }
        .boxed()
    }
    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            fs.remove_file(subvol, device, path.as_pathbuf())?;
            fs.sync_meta_data(device)?;
            subvol.sync_meta_data(fs, device)?;

            Ok(())
        }
        .boxed()
    }
    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            fs.rmdir(subvol, device, path.as_pathbuf())?;
            fs.sync_meta_data(device)?;
            subvol.sync_meta_data(fs, device)?;

            Ok(())
        }
        .boxed()
    }
    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            File::copy(fs, subvol, device, from.as_pathbuf(), to.as_pathbuf())?;

            Ok(())
        }
        .boxed()
    }
    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            fs.rename(subvol, device, from.as_pathbuf(), to.as_pathbuf())?;

            Ok(())
        }
        .boxed()
    }
    fn get_quota(&self) -> FsFuture<'_, (u64, Option<u64>)> {
        async {
            let fs = &mut self.fs.lock().await;

            Ok((
                fs.sb.real_used_blocks * BLOCK_SIZE as u64,
                Some(fs.sb.total_blocks * BLOCK_SIZE as u64),
            ))
        }
        .boxed()
    }
}

#[derive(Debug, Clone, Default)]
struct CoreMetaData {
    size: u64,
    is_dir: bool,
    modified: u64,
    accessed: u64,
    created: u64,
}

impl CoreMetaData {
    fn new<D, P>(
        fs: &mut Filesystem,
        subvol: &mut Subvolume,
        device: &mut D,
        path: P,
    ) -> IOResult<Self>
    where
        D: Read + Write + Seek,
        P: AsRef<Path>,
    {
        let is_dir;
        let size;
        let modified;
        let accessed;
        let created;
        if path.as_ref() == "/" || fs.is_dir(subvol, device, path.as_ref()) {
            let fd = Directory::open(fs, subvol, device, path.as_ref())?;
            is_dir = true;
            size = 0;
            modified = fd.get_inode().mtime;
            accessed = fd.get_inode().atime;
            created = fd.get_inode().ctime;
        } else if fs.is_file(subvol, device, path.as_ref()) {
            let fd = File::open(fs, subvol, device, path.as_ref())?;
            is_dir = false;
            size = fd.get_inode().size;
            modified = fd.get_inode().mtime;
            accessed = fd.get_inode().atime;
            created = fd.get_inode().ctime;
        } else {
            return Err(Error::from(ErrorKind::NotFound));
        }

        Ok(Self {
            size,
            is_dir,
            modified,
            accessed,
            created,
        })
    }
}

impl DavMetaData for CoreMetaData {
    fn len(&self) -> u64 {
        self.size
    }
    fn is_dir(&self) -> bool {
        self.is_dir
    }
    fn modified(&self) -> FsResult<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH + Duration::from_nanos(self.modified))
    }
    fn accessed(&self) -> FsResult<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH + Duration::from_nanos(self.accessed))
    }
    fn created(&self) -> FsResult<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH + Duration::from_nanos(self.created))
    }
}

#[derive(Debug, Default)]
struct CoreDirEntry {
    name: String,
    metadata: CoreMetaData,
}

impl CoreDirEntry {
    fn new<S>(name: S, metadata: CoreMetaData) -> Self
    where
        S: Into<String>,
    {
        Self {
            name: name.into(),
            metadata,
        }
    }
}

impl DavDirEntry for CoreDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.as_bytes().to_owned()
    }
    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let metadata: Box<dyn DavMetaData> = Box::new(self.metadata.clone());
        async { Ok(metadata) }.boxed()
    }
}

async fn dav_handler(req: DavRequest, davhandler: web::Data<DavHandler>) -> DavResponse {
    if let Some(prefix) = req.prefix() {
        let config = DavConfig::new().strip_prefix(prefix);
        davhandler.handle_with(config, req.request).await.into()
    } else {
        davhandler.handle(req.request).await.into()
    }
}

async fn async_quit(
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<FsFile>>,
) {
    let device = &mut device.lock().await as &mut FsFile;
    let fs = &mut fs.lock().await;
    fs.sync_meta_data(device).unwrap();
    subvol.lock().await.sync_meta_data(fs, device).unwrap();
}

fn handle_quit(
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<FsFile>>,
) -> anyhow::Result<()> {
    ctrlc::set_handler(move || {
        tokio::runtime::Runtime::new().unwrap().block_on(async_quit(
            Arc::clone(&fs),
            Arc::clone(&subvol),
            Arc::clone(&device),
        ))
    })?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::init();

    let addr = if std::net::Ipv6Addr::from_str(&args.host).is_ok() {
        format!("[{}]:{}", args.host, args.port)
    } else {
        format!("{}:{}", args.host, args.port)
    };

    let dav_server = DavHandler::builder()
        .filesystem(CoreFilesystem::new(&args.device, args.subvolume)?)
        .locksystem(dav_server::memls::MemLs::new())
        .build_handler();

    tracing::info!("listening on {} serving {}", addr, args.device);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(dav_server.clone()))
            .service(web::resource("/{tail:.*}").to(dav_handler))
    })
    .bind(addr)?
    .run()
    .await?;

    Ok(())
}
