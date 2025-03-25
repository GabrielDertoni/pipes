#![feature(maybe_uninit_slice)]

pub mod ctrl;
mod owning_slice;
pub mod ringbuf;
mod utils;

use std::future::Future;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, io};

use crate::ctrl::{Config, Ctrl};
use crate::owning_slice::OwningSlice;
use crate::utils::SendSafeNonNull;

pub struct Scheduler {
    set: tokio::task::JoinSet<io::Result<()>>,
}

impl Scheduler {
    fn new() -> Self {
        Scheduler {
            set: tokio::task::JoinSet::new(),
        }
    }

    fn spawn<F>(&mut self, f: F)
    where
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.set.spawn(f);
    }

    async fn wait(mut self) -> io::Result<()> {
        while let Some(join_result) = self.set.join_next().await {
            let result = match join_result {
                Ok(result) => result,
                Err(e) => {
                    self.set.abort_all();
                    return Err(io::Error::other(e));
                }
            };
            if let Err(e) = result {
                self.set.abort_all();
                return Err(e);
            }
        }
        Ok(())
    }
}

pub async fn run_pipeline<P>(mut pipeline: P) -> io::Result<()>
where
    P: Pipeline<Output = std::convert::Infallible>,
{
    let mut scheduler = Scheduler::new();
    let ctrl = Arc::new(Ctrl::new(Config::default()));
    let output = PipeWriter::new(Box::new(EmptyPipeWriter { ctrl }));
    pipeline.schedule(output, &mut scheduler);
    scheduler.wait().await
}

pub fn from_iter<I>(iter: I) -> impl Pipeline<Output = I::Item>
where
    I: IntoIterator,
    I::IntoIter: Send + 'static,
    I::Item: Send + 'static,
{
    struct FromIter<I> {
        iter: Option<I>,
    }

    impl<I> Pipeline for FromIter<I>
    where
        I: Iterator + Send + 'static,
        I::Item: Send + 'static,
    {
        type Output = I::Item;

        fn schedule(&mut self, mut output: PipeWriter<Self::Output>, scheduler: &mut Scheduler) {
            let mut iter = self.iter.take().expect("already moved");
            scheduler.spawn(async move {
                'outer: loop {
                    let mut batch = output.batch(None);
                    while batch.capacity_left() > 0 {
                        let Some(item) = iter.next() else {
                            drop(batch);
                            _ = output.flush().await;
                            break 'outer;
                        };
                        batch.push(item).unwrap();
                    }
                    drop(batch);
                    if output.flush().await {
                        break;
                    }
                }
                Ok(())
            });
        }
    }

    FromIter {
        iter: Some(iter.into_iter()),
    }
}

pub fn mapped<I, O>(mut f: impl FnMut(I) -> O + Send + 'static) -> impl Stage<Input = I, Output = O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    try_mapped(move |i| Ok(f(i)))
}

pub fn try_mapped<I, O>(
    mut f: impl FnMut(I) -> io::Result<O> + Send + 'static,
) -> impl Stage<Input = I, Output = O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    from_fn(move |mut input, mut output| async move {
        loop {
            let in_batch = input.next_batch(None).await?;
            let done = in_batch.is_done();
            let mut out_batch = output.batch(in_batch.len());
            for item in in_batch {
                out_batch.push(f(item)?).unwrap();
            }
            drop(out_batch);
            if output.flush().await {
                break;
            }
            if done {
                break;
            }
        }
        Ok(())
    })
}

pub fn from_fn<I, O, F, Fut>(f: F) -> impl Stage<Input = I, Output = O>
where
    F: FnOnce(PipeReader<I>, PipeWriter<O>) -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<()>> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    struct FromFn<I, O, F, Fut> {
        f: F,
        #[allow(clippy::type_complexity)]
        _marker: PhantomData<(fn(I) -> O, Fut)>,
    }

    impl<I, O, F, Fut> Stage for FromFn<I, O, F, Fut>
    where
        F: FnOnce(PipeReader<I>, PipeWriter<O>) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
        I: Send + 'static,
        O: Send + 'static,
    {
        type Input = I;
        type Output = O;
        type Fut = Fut;

        fn run(self, input: PipeReader<I>, output: PipeWriter<O>) -> Fut {
            (self.f)(input, output)
        }
    }

    FromFn::<I, O, F, Fut> {
        f,
        _marker: PhantomData,
    }
}

pub trait Pipeline: Send + 'static {
    type Output: Send + 'static;

    fn schedule(&mut self, output: PipeWriter<Self::Output>, scheduler: &mut Scheduler);

    fn to_dyn(self) -> Box<dyn Pipeline<Output = Self::Output>>
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }

    fn stack<S>(self, stage: S) -> Stacked<Self, S>
    where
        S: Stage<Input = Self::Output>,
        Self: Sized,
    {
        Stacked {
            pipeline: self,
            stage: Some(stage),
        }
    }

    fn for_each<F>(
        self,
        mut f: F,
    ) -> ForEachBatch<
        Self,
        impl for<'a> FnMut(ReadBatch<'a, Self::Output>) -> io::Result<()> + Send + 'static,
    >
    where
        F: FnMut(Self::Output) -> io::Result<()> + Send + 'static,
        Self: Sized,
    {
        self.for_each_batch(move |batch| {
            for item in batch {
                f(item)?;
            }
            Ok(())
        })
    }

    fn for_each_batch<F, O>(self, f: F) -> ForEachBatch<Self, F>
    where
        F: for<'a> FnMut(ReadBatch<'a, O>) -> io::Result<()> + Send + 'static,
        Self: Sized,
    {
        ForEachBatch {
            pipeline: self,
            f: Some(f),
        }
    }

    fn collect<O: FromPipeline<Self::Output>>(self) -> impl Future<Output = io::Result<O>> + Send
    where
        Self: Sized,
    {
        O::from_pipeline(self)
    }
}

pub trait FromPipeline<T>: Sized {
    fn from_pipeline<P: Pipeline<Output = T>>(pipeline: P) -> impl Future<Output = io::Result<Self>> + Send;
}

impl<T: Send + 'static> FromPipeline<T> for Vec<T> {
    fn from_pipeline<P: Pipeline<Output = T>>(pipeline: P) -> impl Future<Output = io::Result<Self>> + Send {
        async fn async_from_pipeline<P: Pipeline>(pipeline: P) -> io::Result<Vec<P::Output>> {
            use tokio::sync::oneshot;

            let (tx, rx) = oneshot::channel();
            let mut vec = Vec::new();
            run_pipeline(
                pipeline.stack(from_fn(move |mut reader, _writer| async move {
                    loop {
                        let batch = reader.next_batch(None).await?;
                        let done = batch.is_done();
                        vec.extend(batch);
                        if done {
                            break;
                        }
                    }
                    _ = tx.send(vec);
                    Ok(())
                })),
            )
            .await?;

            Ok(rx.await.expect("pipeline has not completed entirely"))
        }

        Box::pin(async_from_pipeline(pipeline))
    }
}

impl<O: Send + 'static> Pipeline for Box<dyn Pipeline<Output = O>> {
    type Output = O;

    fn schedule(&mut self, output: PipeWriter<Self::Output>, scheduler: &mut Scheduler) {
        self.as_mut().schedule(output, scheduler);
    }
}

pub trait Stage: Send + 'static {
    type Input;
    type Output: Send + 'static;
    type Fut: Future<Output = io::Result<()>> + Send + 'static;

    fn run(self, input: PipeReader<Self::Input>, output: PipeWriter<Self::Output>) -> Self::Fut;
}

const DEFAULT_BUF_SIZE: usize = 1024;

pub fn default_pipe<T: Send + 'static>() -> (PipeReader<T>, PipeWriter<T>) {
    ringbuf::pipe::<T>(
        DEFAULT_BUF_SIZE,
        Config::default(),
    )
}

pub struct PipeReader<T> {
    inner: Box<dyn PipeReaderImpl<Item = T>>,
}

impl<T: Send + 'static> PipeReader<T> {
    pub(crate) fn new(inner: Box<dyn PipeReaderImpl<Item = T>>) -> Self {
        PipeReader { inner }
    }

    pub async fn next_batch<'a>(
        &'a mut self,
        min_size: impl Into<Option<usize>>,
    ) -> io::Result<ReadBatch<'a, T>> {
        let min_size = min_size.into().unwrap_or(1);
        // NOTE: I don't know how to make this safe.
        let inner: &'a mut dyn PipeReaderImpl<Item = T> = &mut *self.inner;
        let mut ptr = unsafe { SendSafeNonNull::new(NonNull::from(&mut *inner)) };
        let mut inner = Some(inner);
        Ok(std::future::poll_fn(move |cx| {
            match inner.take().unwrap().poll_next_batch(min_size, cx) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    inner = Some(unsafe { ptr.as_mut() });
                    Poll::Pending
                }
            }
        })
        .await)
    }
}

pub struct ReadBatch<'a, T> {
    slice: OwningSlice<'a, T>,
    callback: &'a mut dyn ReadBatchCallback,
    consumed: usize,
    examined: usize,
    done: bool,
}

impl<'a, T> ReadBatch<'a, T> {
    pub fn new(
        consumed: usize,
        examined: usize,
        done: bool,
        slice: OwningSlice<'a, T>,
        callback: &'a mut dyn ReadBatchCallback,
    ) -> Self {
        // TODO: properly implement consumed and examined
        ReadBatch {
            slice,
            callback,
            consumed,
            examined,
            done,
        }
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn len(&self) -> usize {
        self.slice.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Mark elements as consumed and/or examined. Setting `examined` to a
    /// higher value indicates to the pipeline that more input is required
    /// before making further process and conuming existing data.
    pub fn advance_to(&mut self, consumed: usize, examined: usize) {
        assert!(consumed <= examined);
        self.consumed = self.consumed.max(consumed);
        self.examined = self.examined.max(examined);
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.slice.init_slice().iter()
    }
}

impl<'a, T> IntoIterator for ReadBatch<'a, T> {
    type Item = T;
    type IntoIter = read_batch::IntoIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        read_batch::IntoIter::new(self)
    }
}

impl<T> Drop for ReadBatch<'_, T> {
    fn drop(&mut self) {
        self.callback.advance_to(self.consumed, self.examined);
    }
}

mod read_batch {
    use super::ReadBatch;

    pub struct IntoIter<'a, T> {
        inner: ReadBatch<'a, T>,
    }

    impl<'a, T> IntoIter<'a, T> {
        pub(crate) fn new(inner: ReadBatch<'a, T>) -> Self {
            IntoIter { inner }
        }
    }

    impl<'a, T> Iterator for IntoIter<'a, T> {
        type Item = T;

        fn next(&mut self) -> Option<T> {
            if let Some(value) = self.inner.slice.pop_front() {
                self.inner.consumed += 1;
                self.inner.examined += 1;
                Some(value)
            } else {
                None
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.len(), Some(self.len()))
        }
    }

    impl<'a, T> ExactSizeIterator for IntoIter<'a, T> {
        fn len(&self) -> usize {
            self.inner.slice.len()
        }
    }
}

pub trait PipeReaderImpl: Send {
    type Item: Send + 'static;

    fn poll_next_batch<'a>(
        &'a mut self,
        min_size: usize,
        cx: &mut Context,
    ) -> Poll<ReadBatch<'a, Self::Item>>;
}

pub trait ReadBatchCallback: Send {
    fn advance_to(&mut self, consumed: usize, examined: usize);
}

pub struct PipeWriter<T> {
    inner: Box<dyn PipeWriterImpl<Item = T>>,
}

impl<T: Send + 'static> PipeWriter<T> {
    pub(crate) fn new(inner: Box<dyn PipeWriterImpl<Item = T>>) -> Self {
        PipeWriter { inner }
    }

    pub fn batch<'a>(&'a mut self, min_capacity: impl Into<Option<usize>>) -> WriteBatch<'a, T> {
        self.inner.batch(min_capacity.into().unwrap_or(1))
    }

    #[must_use]
    pub async fn flush(&mut self) -> bool {
        std::future::poll_fn(|cx| self.inner.poll_flush(cx)).await
    }
}

pub struct WriteBatch<'a, T> {
    slice: OwningSlice<'a, T>,
    produced: usize,
    callback: &'a mut dyn WriteBatchCallback,
}

impl<'a, T> WriteBatch<'a, T> {
    pub(crate) fn new(
        produced: usize,
        slice: OwningSlice<'a, T>,
        callback: &'a mut dyn WriteBatchCallback,
    ) -> Self {
        WriteBatch {
            produced,
            slice,
            callback,
        }
    }

    /// Writes an element to the batch. If the batch is already full, returns
    /// an error instead.
    pub fn push(&mut self, item: T) -> Result<(), PushErr<T>> {
        match self.slice.push_back(item) {
            Ok(()) => {
                self.produced += 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn len(&self) -> usize {
        self.slice.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of elements that still would fit in this batch.
    pub fn capacity(&self) -> usize {
        self.slice.capacity()
    }

    pub fn capacity_left(&self) -> usize {
        self.capacity() - self.len()
    }
}

impl<T> Drop for WriteBatch<'_, T> {
    fn drop(&mut self) {
        self.callback.produce_to(self.produced);
    }
}

/// The writer side of the pipe. This also makes it possible to control pipeline
/// flow (like knowing when to stop or if the pipeline erroed).
pub trait PipeWriterImpl: Send {
    type Item: Send + 'static;

    /// Gets the next batch to write to. After writing data to the current
    /// batch, `flush` should be called in order to send the batch to the
    /// consuming side of the pipe. This function is allowed to return an empty
    /// batch if called repeatedly without flushing.
    fn batch<'a>(&'a mut self, min_capacity: usize) -> WriteBatch<'a, Self::Item>;

    /// Flushes data to the consuming side of the pipe. Returns an error in case
    /// the pipeline failed or a signal indicating it has completed successfuly.
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<bool>;
}

pub trait WriteBatchCallback: Send {
    fn produce_to(&mut self, produced: usize);
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PushErr<T>(pub T);

impl<T> fmt::Debug for PushErr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PushErr").finish_non_exhaustive()
    }
}

pub struct EmptyPipeWriter {
    ctrl: Arc<Ctrl>,
}

impl PipeWriterImpl for EmptyPipeWriter {
    type Item = std::convert::Infallible;

    fn batch<'a>(&'a mut self, _min_capacity: usize) -> WriteBatch<'a, Self::Item> {
        let produced;
        {
            let ctrl = self.ctrl.lock();
            produced = ctrl.produced();
        }
        WriteBatch::new(produced, OwningSlice::empty(), &mut self.ctrl)
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<bool> {
        self.ctrl.lock().poll_wait_to_produce(cx)
    }
}

pub struct Stacked<P, S> {
    pipeline: P,
    stage: Option<S>,
}

impl<P, S, M> Pipeline for Stacked<P, S>
where
    P: Pipeline<Output = M>,
    S: Stage<Input = M>,
    M: Send + 'static,
{
    type Output = S::Output;

    fn schedule(&mut self, output: PipeWriter<S::Output>, scheduler: &mut Scheduler) {
        let Self { pipeline, stage } = self;
        let stage = stage.take().expect("already scheduled");

        let (intermediate_in, intermediate_out) = default_pipe::<M>();
        pipeline.schedule(intermediate_out, scheduler);
        scheduler.spawn(stage.run(intermediate_in, output));
    }
}

pub struct ForEachBatch<P, F> {
    pipeline: P,
    f: Option<F>,
}

impl<P, F> Pipeline for ForEachBatch<P, F>
where
    P: Pipeline,
    F: for<'a> FnMut(ReadBatch<'a, P::Output>) -> io::Result<()> + Send + 'static,
{
    type Output = std::convert::Infallible;

    fn schedule(&mut self, _output: PipeWriter<Self::Output>, scheduler: &mut Scheduler) {
        let Self { pipeline, f } = self;
        let mut f = f.take().expect("already scheduled");
        let (mut intermediate_in, intermediate_out) = default_pipe::<P::Output>();
        pipeline.schedule(intermediate_out, scheduler);
        scheduler.spawn(async move {
            loop {
                let batch = intermediate_in.next_batch(None).await?;
                let done = batch.is_done();
                f(batch)?;
                if done {
                    break;
                }
            }
            Ok(())
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn can_collect_from_iter() {
        let output = from_iter([1, 2, 3])
            .collect::<Vec<i32>>()
            .await
            .unwrap();

        assert_eq!(output, &[1, 2, 3]);
    }

    #[tokio::test]
    async fn mapped_stages_work() {
        let output = from_iter([1, 2, 3])
            .stack(mapped(|i| i * 2))
            .collect::<Vec<i32>>()
            .await
            .unwrap();

        assert_eq!(output, &[2, 4, 6]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn can_execute_in_multiple_threads() {
        use std::sync::{Arc, Barrier};

        let barrier = Arc::new(Barrier::new(2));
        run_pipeline(
            from_iter([1, 2, 3])
                .stack(from_fn({
                    let barrier = Arc::clone(&barrier);
                    |mut input, mut output| async move {
                        barrier.wait();
                        loop {
                            let batch = input.next_batch(None).await?;
                            let done = batch.is_done();
                            let mut out = output.batch(batch.len());
                            for item in batch {
                                _ = out.push(item);
                            }
                            drop(out);
                            if output.flush().await {
                                break;
                            }
                            if done {
                                break;
                            }
                        }
                        Ok(())
                    }
                }))
                .stack(from_fn(|mut input, _output| async move {
                    barrier.wait();
                    loop {
                        let batch = input.next_batch(None).await?;
                        if batch.is_done() {
                            break;
                        }
                    }
                    Ok(())
                })),
        )
        .await
        .unwrap();
    }
}
