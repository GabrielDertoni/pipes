use std::{fs::File, os::unix::fs::FileExt, sync::Arc};

use bytes::Bytes;
use tokio::sync::oneshot;

use super::{IoDevice, IoOp, IoOpRegistry, SupportedOp};
use crate::IoResult;

pub struct WriteAtOp {
    shared: Arc<WriteAtOpShared>,
}

impl WriteAtOp {
    pub fn new(file: Arc<File>, offset: u64, buf: Bytes) -> Self {
        WriteAtOp { shared: Arc::new(WriteAtOpShared { file, offset, buf }) }
    }
}

struct WriteAtOpShared {
    file: Arc<File>,
    offset: u64,
    buf: Bytes,
}

pub struct WriteAtOpData {
    shared: Arc<WriteAtOpShared>,
    result: oneshot::Receiver<IoResult<usize>>,
}

impl SupportedOp<WriteAtOp> for IoDevice {
    fn dispatch<S>(&mut self, registry: &mut IoOpRegistry<S>, op: WriteAtOp) -> IoResult<()>
    where WriteAtOpData: Into<S>,
    {
        let shared = Arc::clone(&op.shared);
        let (tx, rx) = oneshot::channel();
        let token = registry.save(WriteAtOpData {
            shared: op.shared,
            result: rx,
        });
        let handle = self.handle();
        tokio::task::spawn_blocking(move || {
            _ = tx.send(shared.file.write_at(&shared.buf, shared.offset));
            handle.mark_complete(token);
            _ = handle.wake();
        });
        Ok(())
    }
}

unsafe impl IoOp for WriteAtOp {
    type Output = (IoResult<usize>, Bytes);
    type StoredData = WriteAtOpData;

    unsafe fn complete_into_output(mut data: WriteAtOpData) -> Self::Output {
        let result = data.result.try_recv().expect("is complete");
        (result, data.shared.buf.clone())
    }
}

#[cfg(test)]
mod test {
    use tmpdir::TmpDir;

    use crate::{Pipeline, from_iter, io::IoStage, mapped};

    use super::*;

    #[tokio::test]
    async fn can_write_to_file() {
        let dir = TmpDir::new("can_write_to_file").await.unwrap();
        let dir = dir.to_path_buf();
        let fpath = dir.join("data.txt");
        let contents = "Hello, world!";

        let output = from_iter([fpath.clone()])
            .stack(mapped(move |path| {
                WriteAtOp::new(
                    Arc::new(std::fs::File::create(&path).unwrap()),
                    0,
                    Bytes::from_static(contents.as_bytes()),
                )
            }))
            .stack(IoStage::<WriteAtOp>::new().unwrap())
            .collect::<Vec<(IoResult<usize>, Bytes)>>()
            .await
            .unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0.as_ref().ok(), Some(&contents.len()));
        assert_eq!(str::from_utf8(&output[0].1).unwrap(), contents);
        assert_eq!(std::fs::read_to_string(fpath).unwrap(), contents);
    }
}
