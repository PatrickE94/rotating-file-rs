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
    FileReady(Pin<Box<File>>),
    Swapping,
    Rotating(Pin<Box<dyn Future<Output = Result<Pin<Box<File>>, Error>>>>),
}

pub struct RotatingFile {
    path: PathBuf,
    rotation: RotationMode,
    state: Cell<StateFuture>,
    bytes: usize,
    lines: usize,
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
                lines_at += buf[..n].iter().filter(|x| **x == b'\n').count();
                bytes_at += n;
            }
        }

        let file = Box::pin(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path.as_ref())
                .await?,
        );

        let shouldrotate = match mode {
            RotationMode::Lines(l) => lines_at >= l,
            RotationMode::Bytes(b) => bytes_at >= b,
        };
        let state = if shouldrotate {
            StateFuture::Rotating(Self::rotate(path.as_ref().to_path_buf(), file).boxed())
        } else {
            StateFuture::FileReady(file)
        };

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            rotation: mode,
            lines: lines_at,
            bytes: bytes_at,
            state: Cell::new(state),
        })
    }

    async fn rotate(path: PathBuf, file: Pin<Box<File>>) -> Result<Pin<Box<File>>, Error> {
        file.sync_all().await?;

        let dir = if path.is_file() {
            if let Some(p) = path.parent() {
                PathBuf::from(p)
            } else {
                PathBuf::from(".")
            }
        } else {
            path.clone()
        };
        let target_path = dir.join(Local::now().format("%Y-%m-%d_%H-%M-%S.log").to_string());
        tokio::fs::rename(&path, target_path.clone()).await?;
        Self::compress(target_path.clone()).await?;
        Ok(Box::pin(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(&path)
                .await?,
        ))
    }

    async fn compress(path: PathBuf) -> Result<(), Error> {
        let mut outputfile = path.clone();
        outputfile.set_extension("log.gz");
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

    fn rotate_if_needed(&mut self) {
        let shouldrotate = match self.rotation {
            RotationMode::Bytes(b) => self.bytes >= b,
            RotationMode::Lines(l) => self.lines >= l,
        };

        if shouldrotate {
            match self.state.replace(StateFuture::Swapping) {
                StateFuture::Swapping => {}
                StateFuture::FileReady(file) => {
                    self.state.replace(StateFuture::Rotating(
                        Self::rotate(self.path.clone(), file).boxed(),
                    ));
                }
                StateFuture::Rotating(m) => {
                    self.state.replace(StateFuture::Rotating(m));
                }
            }
        }
    }
}

impl AsyncWrite for RotatingFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let mut me = self.get_mut();
        me.rotate_if_needed();

        loop {
            match me.state.get_mut() {
                StateFuture::FileReady(ref mut file) => {
                    let ret = Pin::as_mut(file).poll_write(cx, buf);
                    if let Poll::Ready(Ok(n)) = ret {
                        me.bytes += n;
                        me.lines += buf[..n].iter().filter(|x| **x == b'\n').count();
                    }
                    return ret;
                }
                StateFuture::Swapping => panic!("Should never happen"),
                StateFuture::Rotating(ref mut fut) => match Pin::as_mut(fut).poll(cx) {
                    Poll::Ready(Ok(file)) => {
                        me.lines = 0;
                        me.bytes = 0;
                        me.state.replace(StateFuture::FileReady(file));
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
            StateFuture::FileReady(ref mut file) => Pin::as_mut(file).poll_flush(cx),
            StateFuture::Rotating(ref mut fut) => match Pin::as_mut(fut).poll(cx) {
                Poll::Ready(Ok(file)) => {
                    me.lines = 0;
                    me.bytes = 0;
                    me.state.replace(StateFuture::FileReady(file));
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
            StateFuture::Swapping => panic!("Should never happen"),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let me = self.get_mut();
        match me.state.get_mut() {
            StateFuture::FileReady(ref mut file) => Pin::as_mut(file).poll_shutdown(cx),
            StateFuture::Rotating(ref mut fut) => match Pin::as_mut(fut).poll(cx) {
                Poll::Ready(Ok(file)) => {
                    me.state.replace(StateFuture::FileReady(file));
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            },
            StateFuture::Swapping => panic!("Should never happen"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn basic_lines() {
        tokio::fs::create_dir_all("./out").await.unwrap();
        let mut file = RotatingFile::new("./out/testfile1.log", RotationMode::Lines(1))
            .await
            .unwrap();
        file.write_all(b"1\n2\n3\n").await.unwrap();
        file.write_all(b"1\n2\n3\n").await.unwrap();
        file.flush().await.unwrap();
    }

    #[tokio::test]
    async fn basic_bytes() {
        tokio::fs::create_dir_all("./out1").await.unwrap();
        let mut file = RotatingFile::new("./out1/testfile.log", RotationMode::Bytes(2))
            .await
            .unwrap();
        file.write_all(b"112233").await.unwrap();
        file.write_all(b"112233").await.unwrap();
        file.flush().await.unwrap();
    }
}
