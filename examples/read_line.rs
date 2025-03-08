use pipes::*;

struct ReadStage;

impl<R, W> Stage<R, W> for ReadStage
where
    R: PipeReader<Item = u8, Err = ()>,
    W: PipeWriter<Item = u8, Err = ()>,
{
    type Input = u8;
    type Output = u8;
    type Err = ();

    type Driver = ErasedDriverFut<Self::Err>;

    fn build(self, mut input: R, mut output: W) -> Self::Driver {
        erased_drive_fut(async move {
            loop {
                let mut batch = input.next_batch().await;
            }
        })
    }
}

fn main() {
}
