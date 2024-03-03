use std::fs::File;
use std::io::Result as IOResult;
use std::io::{Error, ErrorKind};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use actix_web::{web, App, HttpServer};
use clap::Parser;
use dav_server::actix::*;
use dav_server::davpath::*;
use dav_server::fs::*;
use dav_server::*;
use lib31corefs::dir;
use lib31corefs::file;
use lib31corefs::subvol::Subvolume;
use lib31corefs::Filesystem;

macro_rules! open_device {
    ($device: expr) => {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open($device)
            .unwrap()
    };
}

#[derive(Parser)]
#[command(about = "31coreFS webdav interface")]
struct Args {
    device: String,
    #[arg(short, long, default_value = "localhost")]
    host: String,
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
    #[arg(short, long, default_value_t = 0)]
    subvolume: u64,
}

#[derive(Debug)]
struct CoreFile {
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<File>>,
    path: String,
    offset: u64,
    metadata: CoreMetaData,
}

impl CoreFile {
    fn new(
        fs: Arc<Mutex<Filesystem>>,
        subvol: Arc<Mutex<Subvolume>>,
        device: Arc<Mutex<File>>,
        path: &str,
        metadata: CoreMetaData,
    ) -> Self {
        Self {
            fs,
            subvol,
            device,
            path: path.to_owned(),
            metadata,
            offset: 0,
        }
    }
}

impl DavFile for CoreFile {
    fn metadata(&'_ mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        Box::pin(async move { Ok(Box::new(self.metadata.clone()) as Box<dyn DavMetaData>) })
    }
    fn write_buf(&'_ mut self, mut buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        let mut bytes = vec![0; buf.remaining()];
        buf.copy_to_slice(&mut bytes);
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        let mut fd = file::File::open(fs, subvol, device, &self.path).unwrap();
        fd.write(fs, subvol, device, self.offset, &bytes).unwrap();
        fs.sync_meta_data(device).unwrap();

        self.offset += bytes.len() as u64;
        self.metadata.size += bytes.len() as u64;

        Box::pin(async { Ok(()) })
    }

    fn write_bytes(&mut self, buf: bytes::Bytes) -> FsFuture<()> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        let mut fd = file::File::open(fs, subvol, device, &self.path).unwrap();
        fd.write(fs, subvol, device, self.offset, &buf).unwrap();
        fs.sync_meta_data(device).unwrap();

        self.offset += buf.len() as u64;
        self.metadata.size += buf.len() as u64;

        Box::pin(async { Ok(()) })
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<bytes::Bytes> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        let mut fd = file::File::open(fs, subvol, device, &self.path).unwrap();
        let mut bytes = vec![0; count];
        fd.read(fs, subvol, device, self.offset, &mut bytes, count as u64)
            .unwrap();

        self.offset += count as u64;

        Box::pin(async move { Ok(bytes::Bytes::from(bytes)) })
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<u64> {
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
        Box::pin(async { Ok(self.offset) })
    }
    fn flush(&mut self) -> FsFuture<()> {
        Box::pin(async move { Ok(()) })
    }
}

#[derive(Debug, Clone)]
struct CoreFilesystem {
    fs: Arc<Mutex<Filesystem>>,
    subvol: Arc<Mutex<Subvolume>>,
    device: Arc<Mutex<File>>,
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
        Box::pin(async {
            let device = &mut self.device.lock().unwrap() as &mut File;
            let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
            let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

            match CoreMetaData::new(fs, device, &path.to_string()) {
                Ok(metadata) => Ok(Box::new(CoreFile::new(
                    Arc::clone(&self.fs),
                    Arc::clone(&self.subvol),
                    Arc::clone(&self.device),
                    &path.to_string(),
                    metadata,
                )) as Box<dyn DavFile>),
                /* file does not exist, then create it */
                Err(_) => {
                    lib31corefs::file::File::create(fs, subvol, device, &path.to_string()).unwrap();

                    fs.sync_meta_data(device).unwrap();
                    let metadata = CoreMetaData::new(fs, device, &path.to_string()).unwrap();
                    Ok(Box::new(CoreFile::new(
                        Arc::clone(&self.fs),
                        Arc::clone(&self.subvol),
                        Arc::clone(&self.device),
                        &path.to_string(),
                        metadata,
                    )) as Box<dyn DavFile>)
                }
            }
        })
    }
    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<FsStream<Box<dyn DavDirEntry>>> {
        Box::pin(async {
            let device = &mut self.device.lock().unwrap() as &mut File;
            let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
            let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

            let mut fd = dir::Directory::open(fs, subvol, device, &path.to_string()).unwrap();
            let mut v: Vec<Box<dyn DavDirEntry>> = Vec::new();
            for (name, _) in fd.list_dir(fs, subvol, device).unwrap() {
                v.push(Box::new(CoreDirEntry::new(
                    &name,
                    CoreMetaData::new(fs, device, &(path.to_string() + "/" + &name)).unwrap(),
                )));
            }
            let stream = futures_util::stream::iter(v);

            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        })
    }
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<Box<dyn DavMetaData>> {
        Box::pin(async {
            let device = &mut self.device.lock().unwrap() as &mut File;
            let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;

            match CoreMetaData::new(fs, device, &path.to_string()) {
                Ok(metadata) => Ok(Box::new(metadata) as Box<dyn DavMetaData>),
                Err(_) => Err(FsError::NotFound),
            }
        })
    }
    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        lib31corefs::dir::Directory::create(fs, subvol, device, &path.to_string()).unwrap();

        Box::pin(async { Ok(()) })
    }
    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        lib31corefs::file::File::remove(fs, subvol, device, &path.to_string()).unwrap();

        Box::pin(async { Ok(()) })
    }
    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<()> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        lib31corefs::dir::Directory::remove(fs, subvol, device, &path.to_string()).unwrap();

        Box::pin(async { Ok(()) })
    }
    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        lib31corefs::file::File::copy(fs, subvol, device, &from.to_string(), &to.to_string())
            .unwrap();

        Box::pin(async { Ok(()) })
    }
    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<()> {
        let device = &mut self.device.lock().unwrap() as &mut File;
        let fs = &mut self.fs.lock().unwrap() as &mut Filesystem;
        let subvol = &mut self.subvol.lock().unwrap() as &mut Subvolume;

        lib31corefs::rename(fs, subvol, device, &from.to_string(), &to.to_string()).unwrap();

        Box::pin(async { Ok(()) })
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
        if path == "/"
            || lib31corefs::is_dir(fs, &mut fs.get_default_subvolume(device)?, device, path)
        {
            let fd =
                dir::Directory::open(fs, &mut fs.get_default_subvolume(device)?, device, path)?;
            is_dir = true;
            size = 0;
            modified = fd.get_inode().mtime;
            accessed = fd.get_inode().atime;
            created = fd.get_inode().ctime;
        } else if lib31corefs::is_file(fs, &mut fs.get_default_subvolume(device)?, device, path) {
            let fd = file::File::open(fs, &mut fs.get_default_subvolume(device)?, device, path)?;
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
    fn modified(&self) -> FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(self.modified))
    }
    fn accessed(&self) -> FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(self.accessed))
    }
    fn created(&self) -> FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(self.created))
    }
}

#[derive(Debug, Clone, Default)]
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
        Box::pin(async move { Ok(Box::new(self.metadata.clone()) as Box<dyn DavMetaData>) })
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
