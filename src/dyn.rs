use std::task::{Poll, Context};
use std::mem::MaybeUninit;

pub struct StageBuilder<I, O> {
    stage: Box<dyn Stage<Input = I, Output = O>>,
}

impl<I, O> StageBuilder<I, O> {
    fn stack<S, P>(self, next: S) -> StageBuilder<I, O> {
        StageBuilder { stage: Box::new(self.stage.stack(next)) }
    }
}

pub trait Stage {
    type Input;
    type Output;
    type Fut: Future + Send + 'static;

    fn run(self, input: PipeReader<Self::Input>, output: PipeWriter<Self::Output>) -> Self::Fut;
}

fn stage_fn<I, O>(
    f: impl AsyncFnOnce(PipeReader<I>, PipeWriter<O>) -> io::Result<()>
) -> impl Stage<Input = I, Output = O> {
    StageFn { f }
}

struct StageFn<F, I, O> {
    f: F,
    _marker: PhantomData<fn(I) -> O>,
}

impl<F, I, O> Stage for StageFn<F, I, O>
where
    F: AsyncFnOnce(PipeReader<I>, PipeWriter<O>) -> io::Result<()>
{
    type Input = I;
    type Output = O;
    type Fut = F::CallOnceFuture;

    fn run(self, input: PipeReader<I>, output: PipeWriter<O>) -> Fut {
        (self.f)(input, output)
    }
}

impl<S: Stage> Staged for S {
    type Input = S::Input;
    type Output = S::Output;

    fn visit(self, input: PipeReader<Self::Input>, output: PipeWriter<Self::Output>, scheduler: &mut Scheduler) {
        scheduler.spawn(self.run(input, output));
    }
}

pub trait Staged {
    type Input;
    type Output;

    fn visit(self, input: PipeReader<Self::Input>, output: PipeWriter<Self::Output>, scheduler: &mut Scheduler);

    fn stack<S>(self, next: S) -> Stacked<Self, P, S>
    where
        S: Stage<Input = Self::Input>,
        Self: Sized,
    {
        Stacked {
            before: self,
            pipe: Pipe::default(),
            after: next,
        }
    }

    fn connect<P>(self, pipeline: P) -> Connected<Self, P>
    where
        P: Pipeline<Input = Self::Output>,
        Self: Sized,
    {
        Connected { stage: self, pipeline }
    }
}

pub struct PipeReader<T> {
    impl: Box<dyn PipeReaderImpl<Item = T>>,
}

pub struct ReadBatch<'a, T> {
    slice: &'a [T],
    consumed: usize,
    examined: usize,
}

impl<'a, T> ReadBatch<'a, T> {
    /// Mark elements as consumed and/or examined. Setting `examined` to a
    /// higher value indicates to the pipeline that more input is required
    /// before making further process and conuming existing data.
    fn advance_to(&mut self, consumed: usize, examined: usize) {
        assert!(consumed <= examined);
        self.consumed = self.consumed.max(consumed);
        self.examined = self.examined.max(examined);
    }
}

pub trait PipeReaderImpl {
    type Item;

    fn poll_next_batch<'a>(self: Pin<&'a mut Self>, cx: &mut Context) -> Poll<ReadBatch<'a, Self::Item>>;

    /// The function to call when a `Ok(None)` is received from `next_batch`.
    /// This indicates that the consumer is done consuming.
    fn complete(self);
}

pub struct PipeWriter<T> {
    impl: Box<dyn PipeWriterImpl<Item = T>>,
    ctrl: Ctrl,
}

impl<T> PipeWriter<T> {
    async fn flush(&mut self) -> io::Result<()> {
        self.impl.flush()?;
        self.ctrl.finish_batch().await
    }
}

pub struct WriteBatch<'a, T> {
    slice: &'a mut [MaybeUninit<T>],
    len: usize,
}

impl<'a, T> WriteBatch<'a, T> {
    /// Writes an element to the batch. If the batch is already full, returns
    /// an error instead.
    fn push(&mut self, item: T) -> Result<(), PushErr<Self::Item>> {
        if self.len >= self.capacity() {
            return Err(PushErr(item));
        }
        let i = self.len;
        self.slice[i].write(item);
        self.len += 1;
        Ok(())
    }

    /// The number of elements that still would fit in this batch.
    fn capacity(&self) -> usize {
        self.slice.len()
    }
}

/// The writer side of the pipe. This also makes it possible to control pipeline
/// flow (like knowing when to stop or if the pipeline erroed).
pub trait PipeWriterImpl: Send {
    type Item;

    /// Gets the next batch to write to. After writing data to the current
    /// batch, `flush` should be called in order to send the batch to the
    /// consuming side of the pipe. This function is allowed to return an empty
    /// batch if called repeatedly without flushing.
    fn batch<'a>(&'a mut self) -> WriteBatch<'a>;

    /// Flushes data to the consuming side of the pipe. Returns an error in case
    /// the pipeline failed or a signal indicating it has completed successfuly.
    fn flush(&mut self) -> io::Result<()>;
}


pub struct IoUringPipeWriterImpl {
    ring: Rc<RefCell<io_uring::IoUring>>,
}

impl IoUringPipeWriterImpl {
    fn squeue<'a>(&'a mut self) -> RefMut<'a, io_uring::squeue::SubmissionQueue<'a>> {
        self.ring.borrow_mut().map(|ring| ring.submission());
    }
}

impl PipeWriterImpl for IoUringPipeWriterImpl {
    type Item = io_uring::squeue::Sqe;

    /// Gets the next batch to write to. After writing data to the current
    /// batch, `flush` should be called in order to send the batch to the
    /// consuming side of the pipe. This function is allowed to return an empty
    /// batch if called repeatedly without flushing.
    fn batch<'a>(&'a mut self) -> WriteBatch<'a> {
        WriteBatch
    }

    /// Flushes data to the consuming side of the pipe. Returns an error in case
    /// the pipeline failed or a signal indicating it has completed successfuly.
    fn flush(&mut self) -> io::Result<()> {
        self.squeue().sync();
        // self.squeue().submit()
    }
}
