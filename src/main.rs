use std::fs::File as FsFile;
use std::io::{Error, ErrorKind, Result as IOResult};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use actix_web::{web, App, HttpServer};
use clap::Parser;
use dav_server::actix::*;
use dav_server::davpath::*;
use dav_server::fs::*;
use dav_server::*;
use futures_util::FutureExt;
use lib31corefs::dir::Directory;
use lib31corefs::file::File;
use lib31corefs::subvol::Subvolume;
use lib31corefs::Filesystem;
use tokio::sync::Mutex;

macro_rules! open_device {
    ($device: expr) => {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open($device)?
    };
}

#[derive(Parser)]
#[command(about = "31coreFS webdav interface", version)]
struct Args {
    device: String,
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
    /** Default subvolume */
    #[arg(short, long, default_value_t = 0)]
    subvolume: u64,
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
        async move { Ok(Box::new(self.metadata.clone()) as Box<dyn DavMetaData>) }.boxed()
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

    fn write_bytes(&mut self, buf: bytes::Bytes) -> FsFuture<()> {
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

    fn read_bytes(&mut self, count: usize) -> FsFuture<bytes::Bytes> {
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

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<u64> {
        async move {
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
            Ok(self.offset)
        }
        .boxed()
    }
    fn flush(&mut self) -> FsFuture<()> {
        async {
            self.offset = 0;
            Ok(())
        }
        .boxed()
    }
}

#[derive(Debug, Clone)]
struct CoreFilesystem {
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<FsFile>>,
}

impl CoreFilesystem {
    fn new(path: &str, subvol_id: u64) -> IOResult<Box<Self>> {
        let mut device = open_device!(path);
        let fs = Filesystem::load(&mut device)?;
        let sumvol = fs.get_subvolume(&mut device, subvol_id)?;

        Ok(Box::new(Self {
            fs: Arc::new(Mutex::new(fs)),
            device: Arc::new(Mutex::new(device)),
            subvol: Arc::new(Mutex::new(sumvol)),
        }))
    }
}

impl DavFileSystem for CoreFilesystem {
    fn open<'a>(&'a self, path: &'a DavPath, _options: OpenOptions) -> FsFuture<Box<dyn DavFile>> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            match CoreMetaData::new(fs, device, &path.to_string()) {
                Ok(metadata) => Ok(Box::new(CoreFile::new(
                    Arc::clone(&self.fs),
                    Arc::clone(&self.subvol),
                    Arc::clone(&self.device),
                    File::open(fs, subvol, device, &path.to_string())?,
                    metadata,
                )) as Box<dyn DavFile>),
                /* file does not exist, then create it */
                Err(_) => {
                    let fd = File::create(fs, subvol, device, &path.to_string())?;

                    fs.sync_meta_data(device)?;
                    let metadata = CoreMetaData::new(fs, device, &path.to_string())?;
                    Ok(Box::new(CoreFile::new(
                        Arc::clone(&self.fs),
                        Arc::clone(&self.subvol),
                        Arc::clone(&self.device),
                        fd,
                        metadata,
                    )) as Box<dyn DavFile>)
                }
            }
        }
        .boxed()
    }
    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<FsStream<Box<dyn DavDirEntry>>> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            let mut fd = Directory::open(fs, subvol, device, &path.to_string())?;

            let mut v: Vec<Result<Box<dyn DavDirEntry>, FsError>> = Vec::new();
            for (name, _) in fd.list_dir(fs, subvol, device)? {
                v.push(Ok(Box::new(CoreDirEntry::new(
                    &name,
                    CoreMetaData::new(fs, device, &(path.to_string() + "/" + &name))?,
                ))));
            }
            let stream = futures_util::stream::iter(v);

            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;

            if path.to_string().is_empty() {
                return Ok(Box::new(CoreMetaData::new(fs, device, "/")?) as Box<dyn DavMetaData>);
            }

            match CoreMetaData::new(fs, device, &path.to_string()) {
                Ok(metadata) => Ok(Box::new(metadata) as Box<dyn DavMetaData>),
                Err(_) => Err(FsError::NotFound),
            }
        }
        .boxed()
    }
    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            Directory::create(fs, subvol, device, &path.to_string())?;
            fs.sync_meta_data(device)?;

            Ok(())
        }
        .boxed()
    }
    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            File::remove(fs, subvol, device, &path.to_string())?;
            fs.sync_meta_data(device)?;

            Ok(())
        }
        .boxed()
    }
    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            Directory::remove(fs, subvol, device, &path.to_string())?;
            fs.sync_meta_data(device)?;

            Ok(())
        }
        .boxed()
    }
    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            File::copy(fs, subvol, device, &from.to_string(), &to.to_string())?;

            Ok(())
        }
        .boxed()
    }
    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        async {
            let device = &mut self.device.lock().await as &mut FsFile;
            let fs = &mut self.fs.lock().await;
            let subvol = &mut self.subvol.lock().await;

            fs.rename(subvol, device, &from.to_string(), &to.to_string())?;

            Ok(())
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
    fn new<D>(fs: &mut Filesystem, device: &mut D, path: &str) -> IOResult<Self>
    where
        D: Read + Write + Seek,
    {
        let is_dir;
        let size;
        let modified;
        let accessed;
        let created;
        if path == "/" || fs.is_dir(&mut fs.get_default_subvolume(device)?, device, path) {
            let fd = Directory::open(fs, &mut fs.get_default_subvolume(device)?, device, path)?;
            is_dir = true;
            size = 0;
            modified = fd.get_inode().mtime;
            accessed = fd.get_inode().atime;
            created = fd.get_inode().ctime;
        } else if fs.is_file(&mut fs.get_default_subvolume(device)?, device, path) {
            let fd = File::open(fs, &mut fs.get_default_subvolume(device)?, device, path)?;
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
        Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(self.modified))
    }
    fn accessed(&self) -> FsResult<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(self.accessed))
    }
    fn created(&self) -> FsResult<SystemTime> {
        Ok(SystemTime::UNIX_EPOCH + Duration::from_secs(self.created))
    }
}

#[derive(Debug, Default)]
struct CoreDirEntry {
    name: String,
    metadata: CoreMetaData,
}

impl CoreDirEntry {
    fn new(name: &str, metadata: CoreMetaData) -> Self {
        Self {
            name: name.to_owned(),
            metadata,
        }
    }
}

impl DavDirEntry for CoreDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.as_bytes().to_owned()
    }
    fn metadata(&self) -> FsFuture<Box<dyn DavMetaData>> {
        async { Ok(Box::new(self.metadata.clone()) as Box<dyn DavMetaData>) }.boxed()
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let addr = format!("{}:{}", args.host, args.port);

    let dav_server = DavHandler::builder()
        .filesystem(CoreFilesystem::new(&args.device, args.subvolume)?)
        .locksystem(dav_server::memls::MemLs::new())
        .build_handler();

    println!(
        "31corefs-webdav: listening on {} serving {}",
        addr, args.device
    );

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(dav_server.clone()))
            .service(web::resource("/{tail:.*}").to(dav_handler))
    })
    .bind(addr)?
    .run()
    .await
}
