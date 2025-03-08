pub struct Staged<P, S> {
    pipeline: P,
    stage: S,
}

impl<P, S, E> Pipeline for Staged<P, S>
where
    P: Pipeline<Err = E>,
    S: Staged<P::Reader, Input = P1::Output, Err = E>,
{
    type Input = P::Input;
    type Output = S::Output;
    type Err = E;

    type Writer = P::Writer;
    type Reader = S::Reader;
    type Driver = Join<P::Driver, S::Driver>;

    fn split(self) -> (Self::Writer, Self::Reader, Self::Driver) {
        let (writer, reader, pipeline) = self.pipeline.split();
        let (reader, stage) = self.stage.split(p1_reader);
        (writer, reader, join(pipeline, stage))
    }
}


pub trait PipelineRun: Pipeline<Output = ()> {
    async fn run<F, Fut>(self, f: F) -> Result<(), Self::Err>
    where
        F: FnOnce(Self::Writer) -> Fut
        Fut: Future<Output = Result<(), Self::Err>>;
}

impl<P> PipelineRun for P
where
    P: Pipeline<Output = ()>,
{
    type Input = P::Input;
    type Err = P::Err;

    async fn run<F, Fut>(self, f: F) -> Result<(), Self::Err>
    where
        F: FnOnce(Self::Writer) -> Fut
        Fut: Future<Output = Result<(), Self::Err>>,
    {
        let (writer, reader, driver) = self.split();
        join(f(writer), reader.drain(), driver)
    }
}

pub trait Pipeline {
    type Input;
    type Output;
    type Err;

    type Writer: PipeWriter<Item = Self::Input, Err = Self::Err>;
    type Reader: PipeReader<Item = Self::Output, Err = Self::Err>;
    type Driver: Driver<Err = Self::Err>;

    fn split(self) -> (Self::Writer, Self::Reader, Self::Driver);

    fn stage<S>(self, next: S) -> Staged<Self, S>
    where
        S: Stage<Input = Self::Output, Err = Self::Err>,
    {
    }
}

pub trait Driver {
    type Err;

    fn schedule<S>(self, scheduler: &S)
    where
        S: Scheduler<Err = Self::Err>;
}

pub trait Scheduler {
    type Err;

    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = Result<(), Self::Err>>;
}
