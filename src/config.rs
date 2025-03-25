
// TODO: pick better defaults
const DEFAULT_BUF_SIZE: usize = 1024;
const DEFAULT_BACKPRESSURE: usize = 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub buf_size: usize,
    pub backpressure: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            buf_size: DEFAULT_BUF_SIZE,
            backpressure: DEFAULT_BACKPRESSURE,
        }
    }
}
