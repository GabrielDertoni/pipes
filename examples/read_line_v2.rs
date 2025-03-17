use std::{io, fs};

use rayon::prelude::*;
use pipes::*;

async fn stat_files_rayon(input: PipeReader<PathBuf>, output: PipeWriter<fs::Metadata>) -> io::Result<()> {
    while let Some(paths) = input.next_batch(None).await? {
        let metadatas = output.batch(paths.len());
        metadatas.par_extend(batch.par_iter().filter_map(|path| fs::metadata(&path).ok()));
    }
    Ok(())
}

async fn stat_files(input: PipeReader<PathBuf>, output: PipeWriter<fs::Metadata>) -> io::Result<()> {
    while let Some(paths) = input.next_batch(None).await? {
        let metadatas = output.batch(paths.len());
        for path in paths {
            metadatas.push(fs::metadata(&path).ok()).unwrap();
        }
        let done = metadatas.flush().await?;
        if done {
            paths.complete();
            break;
        }
    }
    Ok(())
}

pub enum IoOp {
    Read(ReadOp),
}

pub struct ReadOp {
    path: PathBuf,
    buf: Vec<u8>,
}

async fn read_files(input: PipeReader<fs::Metadata>, output: PipeWriter<IoOp>) -> io::Result<()> {
    while let Some(metadatas) = input.next_batch(None).await? {
        let ops = output.batch(metadatas.len());
        for metadata in metadatas {
            ops.push(ReadOp::new(metada.path(), Vec::with_capacity(metadata.len())))
        }
        ops.flush();
    }
}

fn mapped<I, O>(f: FnMut(I) -> O) -> impl Stage<Input = I, Output = O> {

}

fn main() {
}
