pub mod read;
pub mod write;

use std::{io::Result as IoResult, sync::{Arc, Mutex}};

use slab::Slab;

use crate::{PipeReader, PipeWriter, Stage};

pub unsafe trait IoOp {
    type Output;
    type StoredData;

    unsafe fn complete_into_output(data: Self::StoredData) -> Self::Output;
}

const WAKER_TOKEN: mio::Token = mio::Token(0);

pub struct IoDevice {
    poll: Option<mio::Poll>,
    shared: Arc<IoDeviceShared>,
}

pub struct IoDeviceHandle {
    shared: Arc<IoDeviceShared>,
}

impl IoDeviceHandle {
    pub fn wake(&self) -> IoResult<()> {
        self.shared.waker.wake()
    }

    pub fn mark_complete(&self, token: mio::Token) {
        self.shared.ready.lock().unwrap().push(token);
    }
}

pub struct IoDeviceShared {
    ready: Mutex<Vec<mio::Token>>,
    waker: mio::Waker,
}

impl IoDevice {
    pub fn new() -> IoResult<Self> {
        let poll = mio::Poll::new()?;
        let waker = mio::Waker::new(poll.registry(), WAKER_TOKEN)?;
        Ok(IoDevice {
            poll: Some(poll),
            shared: Arc::new(IoDeviceShared { ready: Mutex::new(Vec::new()), waker })
        })
    }

    pub fn handle(&self) -> IoDeviceHandle {
        IoDeviceHandle { shared: Arc::clone(&self.shared) }
    }

    async fn wait_for_events(&mut self, events: &mut Vec<mio::Token>) -> IoResult<()> {
        let mut owned = std::mem::take(events);
        let shared = Arc::clone(&self.shared);
        let mut poll = self.poll.take().unwrap();
        let result = tokio::task::spawn_blocking(move || {
            let mut events = mio::Events::with_capacity(owned.capacity());
            if let Err(e) = poll.poll(&mut events, None) {
                return Err((e, poll));
            }
            for ev in events.iter() {
                if ev.token() == WAKER_TOKEN {
                    owned.append(&mut *shared.ready.lock().unwrap());
                } else {
                    owned.push(ev.token());
                }
            }
            Ok((owned, poll))
        }).await.unwrap();
        match result {
            Ok((owned, poll)) => {
                *events = owned;
                self.poll = Some(poll);
                Ok(())
            }
            Err((e, poll)) => {
                self.poll = Some(poll);
                Err(e)
            }
        }
    }
}

pub trait SupportedOp<O: IoOp> {
    fn dispatch<S>(&mut self, registry: &mut IoOpRegistry<S>, op: O) -> IoResult<()>
    where
        O::StoredData: Into<S>;
}

pub struct IoOpRegistry<O> {
    data: Slab<O>,
}

impl<O> IoOpRegistry<O> {
    pub fn save<S: Into<O>>(&mut self, data: S) -> mio::Token {
        mio::Token(self.data.insert(data.into()))
    }

    pub fn take(&mut self, token: mio::Token) -> O {
        self.data.remove(token.0 as usize)
    }
}

impl<O> IoOpRegistry<O> {
    fn new() -> Self {
        IoOpRegistry { data: Slab::new() }
    }
}

pub struct IoStage<O: IoOp> {
    device: IoDevice,
    registry: IoOpRegistry<O::StoredData>,
}

impl<O: IoOp> IoStage<O> {
    pub fn new() -> IoResult<Self> {
        Ok(IoStage {
            device: IoDevice::new()?,
            registry: IoOpRegistry::new(),
        })
    }
}

impl<O> Stage for IoStage<O>
where
    O: IoOp + Send + 'static,
    O::Output: Send + 'static,
    O::StoredData: Send + 'static,
    IoDevice: SupportedOp<O>,
{
    type Input = O;
    type Output = O::Output;

    async fn run(
        mut self,
        mut input: PipeReader<O>,
        mut output: PipeWriter<O::Output>,
    ) -> IoResult<()> {
        const BATCH_SIZE: usize = 10;
        let mut input_done = false;
        let mut in_flight: usize = 0;
        'outer: loop {
            if !input_done {
                let in_batch = input.next_batch(None).await?;
                input_done = in_batch.is_done();
                for op in in_batch {
                    self.device.dispatch(&mut self.registry, op)?;
                    in_flight += 1;
                }
            }
            let mut events = Vec::with_capacity(BATCH_SIZE);
            self.device.wait_for_events(&mut events).await?;
            let mut out_batch = output.batch(BATCH_SIZE);
            for token in events {
                let mut value = unsafe { O::complete_into_output(self.registry.take(token)) };
                while let Err(v) = out_batch.push(value) {
                    drop(out_batch);
                    if output.flush().await {
                        break 'outer;
                    }
                    out_batch = output.batch(BATCH_SIZE);
                    value = v.0;
                }
                in_flight -= 1;
            }
            drop(out_batch);
            if output.flush().await || (input_done && in_flight == 0) {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{Pipeline, from_iter, mapped};

    use super::*;

    #[tokio::test]
    async fn can_pass_through_io_layer() {
        struct MockIoOp(usize);

        unsafe impl IoOp for MockIoOp {
            type Output = usize;
            type StoredData = usize;

            unsafe fn complete_into_output(data: Self::StoredData) -> Self::Output {
                data
            }
        }

        impl SupportedOp<MockIoOp> for IoDevice {
            fn dispatch<S>(&mut self, registry: &mut IoOpRegistry<S>, op: MockIoOp) -> IoResult<()>
            where
                usize: Into<S>,
            {
                let token = registry.save(op.0);
                let handle = self.handle();
                handle.mark_complete(token);
                handle.wake().unwrap();
                Ok(())
            }
        }

        let output = from_iter([1, 2, 3])
            .stack(mapped(MockIoOp))
            .stack(IoStage::<MockIoOp>::new().unwrap())
            .collect::<Vec<usize>>()
            .await
            .unwrap();

        assert_eq!(output, &[1, 2, 3]);
    }
}
