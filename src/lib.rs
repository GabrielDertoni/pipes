#![feature(slice_ptr_get, maybe_uninit_slice)]

pub mod ringbuf;
pub mod ctrl;

use std::future::Future;
use std::task::{Poll, Context};
use std::pin::Pin;

/// A stage performs some compute work. It reads from the reader side of a pipe
/// and outputs to a writer side with the data transformed.
pub trait Stage<R, W>
where
    R: PipeReader<Item = Self::Input, Err = Self::Err>,
    W: PipeWriter<Item = Self::Output, Err = Self::Err>,
{
    type Input;
    type Output;
    type Err;

    type Driver: Driver<Err = Self::Err>;

    fn build(self, input: R, output: W) -> Self::Driver;

    fn stage<S, P>(self, next: S, pipe: P) -> Stacked<Self, P, S>
    where
        P: Pipe<Item = Self::Output>,
        /* S: IsStage */
        Self: Sized,
    {
        Stacked {
            before: self,
            pipe,
            after: next,
        }
    }

    fn connect<P>(self, pipeline: P) -> Connected<Self, P>
    where
        P: Pipeline<Input = Self::Output, Err = Self::Err>,
        Self: Sized,
    {
        Connected { stage: self, pipeline }
    }
}

/// Similar to a `Stage`, but instead of taking reader and writer as parameters
/// it decides on those types. This is useful for interacting with systems that
/// don't need intermediate pipes connecting them (e.g. IO). A typical
/// implementation of a pipeline would be an io_uring ring.
pub trait Pipeline {
    type Input;
    type Output;
    type Err;

    type Writer: PipeWriter<Item = Self::Input, Err = Self::Err>;
    type Reader: PipeReader<Item = Self::Output, Err = Self::Err>;
    type Driver: Driver<Err = Self::Err>;

    fn split(self) -> (Self::Writer, Self::Reader, Self::Driver);
}

/// A driver is essentially a builder to a future or group of futures. It
/// encodes the actual computation work that must be done in order to drive a
/// pipeline.
pub trait Driver: Send {
    type Err;

    /// Schedules the current driver. This is essentially a visitor pattern
    /// where `scheduler` is the visitor.
    fn schedule<S>(self, scheduler: &S)
    where
        S: Scheduler<Err = Self::Err>;
}

/// A scheduler traverses through drivers and decides how to spawn their
/// futures. The scheduler may decide to schedule either in separate threads or
/// on the same one or anywhere in between.
pub trait Scheduler {
    type Err;

    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = Result<(), Self::Err>> + Send + 'static;
}

/// A pipe is essentially a buffer that has a write end and a read end. Data
/// that is written to the writer is able to be consumed by the reader in
/// batches. The pipe structure itself usually won't do much itself but it
/// rather serves as a way to pack a reader and a writer that are connected in
/// a single structure. A typical implementation of a pipe would be a ring
/// buffer or a channel of `Vec<Item>`.
pub trait Pipe: Send {
    type Item;
    type Err;
    type Writer: PipeWriter<Item = Self::Item, Err = Self::Err>;
    type Reader: PipeReader<Item = Self::Item, Err = Self::Err>;

    fn split(self) -> (Self::Writer, Self::Reader);
}

/// The writer side of the pipe. This also makes it possible to control pipeline
/// flow (like knowing when to stop or if the pipeline erroed).
pub trait PipeWriter: Send {
    type Item;
    type Err;
    type Batch<'a>: WriteBatch<Item = Self::Item> + 'a
    where
        Self: 'a;

    /// Gets the next batch to write to. After writing data to the current
    /// batch, `flush` should be called in order to send the batch to the
    /// consuming side of the pipe. This function is allowed to return an empty
    /// batch if called repeatedly without flushing.
    fn batch<'a>(&'a mut self) -> Self::Batch<'a>;

    /// Flushes data to the consuming side of the pipe. Returns an error in case
    /// the pipeline failed or a signal indicating it has completed successfuly.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<FlushResult>;

    /// Mark this producer as complete, signaling that no more data will be
    /// sent. This method should be preferred rather than dropping the writer.
    fn complete(self);

    fn fail(self, err: Self::Err);

    async fn enter<F>(mut self, f: F)
    where
        F: AsyncFnOnce(&mut Self) -> Result<(), Self::Err>,
        Self: Sized,
    {
        match f(&mut self).await {
            Ok(()) => self.complete(),
            Err(err) => self.fail(err),
        }
    }
}

pub trait PipWriterExt: PipeWriter + Sized {
    fn flush(&mut self) -> Flush<'_, Self>
    where
        Self: Unpin,
    {
        assert_future_with_output::<_, FlushResult>(Flush { pipe: self })
    }
}

impl<T: PipeWriter> PipWriterExt for T {}

/// The reader side of the pipe. This also makes it possible to control pipeline
/// flow (like knowing when to stop or if the pipeline erroed).
pub trait PipeReader: Send {
    type Item;
    type Err;
    type Batch<'a>: ReadBatch<Item = Self::Item> + 'a
    where
        Self: 'a;

    fn poll_next_batch<'a>(self: Pin<&'a mut Self>, cx: &mut Context) -> Poll<Self::Batch<'a>>;

    /// The function to call when a `Ok(None)` is received from `next_batch`.
    /// This indicates that the consumer is done consuming.
    fn complete(self);

    fn fail(self, err: Self::Err);
}

pub trait PipeReaderExt: PipeReader + Sized {
    fn next_batch<'a>(&'a mut self) -> NextBatch<'a, Self>
    where
        Self: Unpin,
    {
        assert_future_with_output::<_, Self::Batch<'a>>(NextBatch { pipe: Some(self) })
    }

    async fn enter<F>(mut self, f: F)
    where
        F: AsyncFnOnce(&mut Self) -> Result<(), Self::Err>,
        Self: Sized,
    {
        match f(&mut self).await {
            Ok(()) => self.complete(),
            Err(err) => self.fail(err),
        }
    }
}

impl<T: PipeReader> PipeReaderExt for T {}

/// A batch of data coming from a `PipeReader`. The positions and indicies used
/// by the batch reflect global positions throught the lifetime of the pipeline
/// rather than being interior to this batch. This allows for more easily
/// implementing logic that may cross batch boundaries.
pub trait ReadBatch {
    type Item;

    /// The position of the next item relative to the entire pipeline.
    fn pos(&self) -> usize;

    /// Return the element at `pos()` and advance the position.
    fn next(&mut self) -> Option<Self::Item>;

    /// Mark elements as consumed and/or examined. Setting `examined` to a
    /// higher value indicates to the pipeline that more input is required
    /// before making further process and conuming existing data.
    fn advance_to(&mut self, consumed: usize, examined: usize);

    fn get(&self, index: usize) -> Option<&Self::Item>;
    fn get_mut(&mut self, index: usize) -> Option<&mut Self::Item>;

    /// The number of unconsumed elements in the current batch.
    fn len(&self) -> usize;

    fn is_complete(&self) -> bool;

    fn cur_slice(&self) -> &[Self::Item] {
        if let Some(el) = self.get(0) {
            std::slice::from_ref(el)
        } else {
            &[]
        }
    }
}

pub trait WriteBatch {
    type Item;

    /// Writes an element to the batch. If the batch is already full, returns
    /// an error instead.
    fn push(&mut self, item: Self::Item) -> Result<(), PushErr<Self::Item>>;

    /// The number of elements that still would fit in this batch.
    fn capacity(&self) -> usize;

    fn push_slice(&mut self, items: &[Self::Item]) -> Result<(), PushErr<&[Self::Item]>>
    where
        Self::Item: Copy,
    {
        let mut items = items;
    }
}

pub enum FlushResult {
    Yield,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushErr<T>(T);

pub type ErasedDriverFut<E> = DriverFut<Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'static>>>;

pub fn erased_drive_fut<F, E>(fut: F) -> ErasedDriverFut<E>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
{
    DriverFut { fut: Box::pin(fut) }
}

pub fn drive_fut<F, E>(fut: F) -> DriverFut<F>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
{
    DriverFut { fut }
}

pub struct DriverFut<F> {
    fut: F,
}

impl<F, E> Driver for DriverFut<F>
where
    F: Future<Output = Result<(), E>> + Send + 'static,
{
    type Err = E;

    /// Schedules the current driver. This is essentially a visitor pattern
    /// where `scheduler` is the visitor.
    fn schedule<S>(self, scheduler: &S)
    where
        S: Scheduler<Err = Self::Err>,
    {
        scheduler.spawn(async move { self.fut.await })
    }
}

pub struct Stacked<S1, P, S2> {
    before: S1,
    pipe: P,
    after: S2,
}

impl<R, W, S1, P, S2, E, I, O> Stage<R, W> for Stacked<S1, P, S2>
where
    R: PipeReader<Item = I, Err = E>,
    W: PipeWriter<Item = O, Err = E>,
    P: Pipe<Err = E>,
    S1: Stage<R, P::Writer, Input = I, Output = P::Item, Err = E>,
    S2: Stage<P::Reader, W, Input = P::Item, Output = O, Err = E>,
{
    type Input = I;
    type Output = O;
    type Err = E;

    type Driver = Join<S1::Driver, S2::Driver>;

    fn build(self, reader: R, writer: W) -> Self::Driver {
        let (pipe_writer, pipe_reader) = self.pipe.split();
        let before = self.before.build(reader, pipe_writer);
        let after = self.after.build(pipe_reader, writer);
        Join::new(before, after)
    }
}

pub struct Connected<S, P> {
    stage: S,
    pipeline: P,
}

pub struct Join<D1, D2> {
    d1: D1,
    d2: D2,
}

impl<D1, D2> Join<D1, D2> {
    pub fn new(d1: D1, d2: D2) -> Self {
        Join { d1, d2 }
    }
}

impl<D1, D2, E> Driver for Join<D1, D2>
where
    D1: Driver<Err = E>,
    D2: Driver<Err = E>,
{
    type Err = E;

    fn schedule<S>(self, scheduler: &S)
    where
        S: Scheduler<Err = Self::Err>,
    {
        self.d1.schedule(scheduler);
        self.d2.schedule(scheduler);
    }
}

pub struct Flush<'a, P> {
    pipe: &'a mut P,
}

pub struct NextBatch<'a, P> {
    pipe: Option<&'a mut P>,
}

impl<'a, P: PipeReader + Unpin> Future for NextBatch<'a, P> {
    type Output = P::Batch<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<P::Batch<'a>> {
        use std::ptr::NonNull;

        let p: &'a mut P = &mut *self.pipe.take().expect("polled after completion");
        let mut ptr = NonNull::from(&mut *p);
        if let Poll::Ready(res) = Pin::new(p).poll_next_batch(cx) {
            return Poll::Ready(res);
        }
        // SAFETY: Since `poll_next_batch` returned `Pending`, we know the ref
        // is still valid.
        // NOTE: It's a bit unfortunate that I didn't find another way of doing
        // this that doesn't require unsafe.
        self.pipe = Some(unsafe { ptr.as_mut() });
        Poll::Pending
    }
}

fn assert_future_with_output<F: Future<Output = O>, O>(f: F) -> F { f }
