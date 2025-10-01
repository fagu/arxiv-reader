use std::time::{Duration, Instant};

pub struct Client {
    last_request: Option<Instant>,
    inner: reqwest::blocking::Client,
}

impl Client {
    pub fn new() -> Self {
        Self {
            last_request: None,
            inner: reqwest::blocking::Client::new(),
        }
    }

    /// Calls f with the inner reqwest::blocking::Client.
    /// Sleeps if necessary to make sure that at least 3 seconds passed since the completion of the last call to this function.
    pub fn with<T>(&mut self, f: impl FnOnce(&reqwest::blocking::Client) -> T) -> T {
        let now = Instant::now();
        if let Some(last_request) = self.last_request
            && let Some(remaining) =
                Duration::from_secs(3).checked_sub(now.duration_since(last_request))
        {
            println!("Waiting for {:.2} seconds.", remaining.as_secs_f32());
            std::thread::sleep(remaining);
        }
        let res = f(&self.inner);
        self.last_request = Some(Instant::now());
        res
    }
}
