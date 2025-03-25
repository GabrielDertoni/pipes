use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::{Arc, Mutex},
};

use bytes::BytesMut;
use tokio::sync::oneshot;

use super::{IoDevice, IoOp, IoOpRegistry, SupportedOp};
use crate::IoResult;

pub struct ReadAtOp {
    shared: Arc<ReadAtOpShared>,
}

impl ReadAtOp {
    pub fn new(file: Arc<File>, offset: u64, size: usize, buf: BytesMut) -> Self {
        ReadAtOp {
            shared: Arc::new(ReadAtOpShared {
                file,
                offset,
                size,
                buf: Mutex::new(buf),
            }),
        }
    }
}

struct ReadAtOpShared {
    file: Arc<File>,
    offset: u64,
    size: usize,
    buf: Mutex<BytesMut>,
}

pub struct ReadAtOpData {
    shared: Arc<ReadAtOpShared>,
    result: oneshot::Receiver<IoResult<usize>>,
}

impl SupportedOp<ReadAtOp> for IoDevice {
    fn dispatch<S>(&mut self, registry: &mut IoOpRegistry<S>, op: ReadAtOp) -> IoResult<()>
    where
        ReadAtOpData: Into<S>,
    {
        let shared = Arc::clone(&op.shared);
        let (tx, rx) = oneshot::channel();
        let token = registry.save(ReadAtOpData {
            shared: op.shared,
            result: rx,
        });
        let handle = self.handle();
        tokio::task::spawn_blocking(move || {
            let mut buf = shared.buf.lock().unwrap();
            let mut buf = &mut *buf;
            buf.resize(shared.size, 0);
            _ = tx.send(match shared.file.read_at(&mut buf, shared.offset) {
                Ok(read) => {
                    buf.truncate(read);
                    Ok(read)
                },
                Err(e) => {
                    buf.truncate(0);
                    Err(e)
                }
            });
            handle.mark_complete(token);
            _ = handle.wake();
        });
        Ok(())
    }
}

unsafe impl IoOp for ReadAtOp {
    type Output = (IoResult<usize>, BytesMut);
    type StoredData = ReadAtOpData;

    unsafe fn complete_into_output(mut data: ReadAtOpData) -> Self::Output {
        let result = data.result.try_recv().expect("is complete");
        (result, data.shared.buf.lock().unwrap().split())
    }
}

#[cfg(test)]
mod test {
    use tmpdir::TmpDir;

    use crate::{Pipeline, from_iter, io::IoStage, mapped};

    use super::*;

    #[tokio::test]
    async fn can_read_from_file() {
        let dir = TmpDir::new("can_read_from_file").await.unwrap();
        let dir = dir.to_path_buf();
        let fpath = dir.join("data.txt");
        let contents = "Hello, world!";
        std::fs::write(&fpath, contents).unwrap();

        let output = from_iter([fpath.clone()])
            .stack(mapped(move |path| {
                ReadAtOp::new(
                    Arc::new(std::fs::File::open(&path).unwrap()),
                    0,
                    128,
                    BytesMut::new(),
                )
            }))
            .stack(IoStage::<ReadAtOp>::new().unwrap())
            .collect::<Vec<(IoResult<usize>, BytesMut)>>()
            .await
            .unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0.as_ref().ok(), Some(&contents.len()));
        assert_eq!(str::from_utf8(&output[0].1).unwrap(), contents);
    }
}
