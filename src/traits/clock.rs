use std::time::{Duration, Instant};

pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}
