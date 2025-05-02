#![doc = include_str!("../README.md")]
#![feature(mpmc_channel)]

use std::{
    sync::{Arc, Condvar, Mutex},
    thread::scope,
};

pub use ordered::*;
pub use reduction::*;
pub use unordered::*;

pub mod ordered;
pub mod reduction;
pub mod unordered;

#[derive(Clone)]
struct Gate<S>(Arc<(Mutex<S>, Condvar)>);

impl<S> Gate<S> {
    fn new(initial_state: S) -> Self {
        Self(Arc::new((Mutex::new(initial_state), Condvar::new())))
    }

    fn update(&self, updater: impl Fn(&mut S)) {
        let mut state = self.0.0.lock().unwrap();
        (updater)(&mut state);
        self.0.1.notify_all();
    }

    fn wait_while(&self, condition: impl Fn(&S) -> bool) {
        let mut state = self.0.0.lock().unwrap();
        while (condition)(&state) {
            state = self.0.1.wait(state).unwrap();
        }
    }

    fn check(&self) -> S
    where
        S: Copy,
    {
        *self.0.0.lock().unwrap()
    }
}

/// Extension trait to provide the `filter_map_reduce_async` function
/// for iterators.
pub trait FilterMapReduceAsync: Iterator {
    /// Combine multithreaded mapping, filtering, and reduction all into a single tidy function call.
    ///
    /// Makes use of the standard [`Threadpool`] to discard the unnecessary overhead introduced by
    /// the [`OrderedThreadpool`], which is unnecessary in light of the fact that all the results
    /// end up being reduced down to a single value anyway.
    ///
    /// ```
    /// use threadpool::*;
    ///
    /// let vals = 0..10000usize;
    ///
    /// let sequential_result = vals
    ///     .clone()
    ///     .filter_map(|x: usize| {
    ///         let x = x.pow(3) % 100;
    ///         (x > 50).then_some(x)
    ///     })
    ///     .reduce(|a, b| a + b)
    ///     .unwrap();
    ///
    /// let parallel_result = vals
    ///     .filter_map_reduce_async(
    ///         |x: usize| {
    ///             let x = x.pow(3) % 100;
    ///             (x > 50).then_some(x)
    ///         },
    ///         |a, b| a + b,
    ///     )
    ///     .unwrap();
    ///
    /// assert_eq!(sequential_result, parallel_result);
    /// ```
    fn filter_map_reduce_async<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync;
}

impl<T> FilterMapReduceAsync for T
where
    T: Iterator + Send + Sync,
{
    fn filter_map_reduce_async<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync,
    {
        scope(|scope| {
            let pool = Threadpool::new(f, scope);
            pool.producer(self);
            pool.into_iter().reduce_async(r)
        })
    }
}
