use chrono::prelude::*;
use flate2::{write::GzEncoder, Compression};
use futures::{
    future::Future,
    task::{Context, Poll},
    FutureExt,
};
use std::{
    cell::Cell,
    io::{Error, Write},
    path::{Path, PathBuf},
    pin::Pin,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWrite},
};

#[derive(Clone, Copy)]
pub enum RotationMode {
    Lines(usize),
    Bytes(usize),
}

enum StateFuture {
    FileReady(usize, usize),
    Rotating(Pin<Box<dyn Future<Output = Result<Pin<Box<File>>, Error>> + Send>>),
}

pub struct RotatingFile {
    path: PathBuf,
    rotation: RotationMode,
    state: Cell<StateFuture>,
    file: Option<Pin<Box<File>>>,
}

fn countlines(buf: &[u8]) -> usize {
    buf.iter().filter(|x| **x == b'\n').count()
}

impl RotatingFile {
    pub async fn new<P: AsRef<Path>>(path: P, mode: RotationMode) -> Result<Self, Error> {
        // Count existing details if file exists
        let mut lines_at: usize = 0;
        let mut bytes_at: usize = 0;
        if let Ok(mut existing) = File::open(path.as_ref()).await {
            let mut buf = [0u8; 1024];
            while let Ok(n) = existing.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                lines_at += countlines(&buf[..n]);
                bytes_at += n;
            }
        }

        if let Some(dir) = path.as_ref().parent() {
            tokio::fs::create_dir_all(dir).await?;
        }

        let file = Box::pin(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path.as_ref())
                .await?,
        );

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            rotation: mode,
            state: Cell::new(StateFuture::FileReady(lines_at, bytes_at)),
            file: Some(file),
        })
    }

    fn get_rotate_dest(path: &Path) -> PathBuf {
        let mut path = path.to_path_buf();
        let extension = path.extension().unwrap().to_owned();
        path.set_extension("");
        let mut filename = path.file_name().unwrap().to_owned(); // Must be file in constructor
        filename.push(Local::now().format("-%Y-%m-%d_%H-%M-%S").to_string());
        path.set_file_name(filename);
        path.set_extension(extension);
        println!("{:?}", path);
        path
    }

    async fn rotate_fut(path: PathBuf, file: Pin<Box<File>>) -> Result<Pin<Box<File>>, Error> {
        file.sync_all().await?;

        let target_path = Self::get_rotate_dest(&path);
        tokio::fs::rename(&path, &target_path).await?;
        Self::compress(&target_path).await?;
        Ok(Box::pin(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(&path)
                .await?,
        ))
    }

    async fn compress(path: &Path) -> Result<(), Error> {
        let mut outputfile = path.to_path_buf();
        let mut extension = outputfile.extension().unwrap().to_owned();
        extension.push(".gz");
        outputfile.set_extension(extension);

        let mut inputfile = File::open(&path).await?;
        let mut gz = GzEncoder::new(
            std::fs::File::create(outputfile.clone())?,
            Compression::fast(),
        );
        loop {
            let mut buffer = [0_u8; 4096];
            let n = inputfile.read(&mut buffer[..]).await?;
            if n == 0 {
                gz.finish()?;
                tokio::fs::remove_file(&path).await?;
                break;
            } else {
                gz.write_all(&buffer[..n])?;
            }
        }
        Ok(())
    }

    fn rotate(&mut self) {
        self.state.replace(StateFuture::Rotating(
            Self::rotate_fut(self.path.clone(), self.file.take().unwrap()).boxed(),
        ));
    }
}

impl AsyncWrite for RotatingFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let me = self.get_mut();

        loop {
            match me.state.get_mut() {
                StateFuture::FileReady(ref mut lines, ref mut bytes) => {
                    let shouldrotate = match me.rotation {
                        RotationMode::Lines(l) => *lines >= l,
                        RotationMode::Bytes(b) => *bytes >= b,
                    };
                    if shouldrotate {
                        me.rotate();
                        continue;
                    } else {
                        let ret = Pin::as_mut(me.file.as_mut().unwrap()).poll_write(cx, buf);
                        if let Poll::Ready(Ok(n)) = ret {
                            *bytes += n;
                            *lines += countlines(&buf[..n]);
                        }
                        return ret;
                    }
                }
                StateFuture::Rotating(ref mut fut) => match Pin::as_mut(fut).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        me.file = Some(file);
                        me.state.replace(StateFuture::FileReady(0, 0));
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                },
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let me = self.get_mut();
        match me.state.get_mut() {
            StateFuture::FileReady(_, _) => Pin::as_mut(me.file.as_mut().unwrap()).poll_flush(cx),
            StateFuture::Rotating(ref mut fut) => match Pin::as_mut(fut).poll(cx) {
                Poll::Ready(Ok(file)) => {
                    me.file = Some(file);
                    me.state.replace(StateFuture::FileReady(0, 0));
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let me = self.get_mut();
        match me.state.get_mut() {
            StateFuture::FileReady(_, _) => {
                Pin::as_mut(me.file.as_mut().unwrap()).poll_shutdown(cx)
            }
            StateFuture::Rotating(ref mut fut) => match Pin::as_mut(fut).poll(cx) {
                Poll::Ready(Ok(file)) => {
                    me.file = Some(file);
                    me.state.replace(StateFuture::FileReady(0, 0));
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn enforce_file_path() {
        let file = RotatingFile::new(".", RotationMode::Lines(1)).await;
        assert!(file.is_err());
    }

    #[tokio::test]
    async fn basic_lines() {
        let mut file = RotatingFile::new("./out/testfile1.log", RotationMode::Lines(1))
            .await
            .unwrap();
        file.write_all(b"1\n2\n3\n").await.unwrap();
        file.write_all(b"1\n2\n3\n").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn basic_bytes() {
        let mut file = RotatingFile::new("./out1/testfile.log", RotationMode::Bytes(2))
            .await
            .unwrap();
        file.write_all(b"112233").await.unwrap();
        file.write_all(b"112233").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn send() {
        tokio::spawn(async {
            tokio::fs::create_dir_all("./out1").await.unwrap();
            let file = RotatingFile::new("./out1/testfile.log", RotationMode::Bytes(2))
                .await
                .unwrap();
            let mut buf = tokio::io::BufWriter::with_capacity(500, file);
            buf.write_all(b"112233").await.unwrap();
            buf.write_all(b"112233").await.unwrap();
            let mut file = buf.into_inner();
            file.flush().await.unwrap();
        });
    }
}
