//! Ordered thread pool implementation.
//!
//! See [`OrderedThreadpool`] for more detailed documentation.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        mpmc::{IntoIter, Receiver, Sender, channel},
        mpsc::TryRecvError,
    },
    thread::{self, Scope, ScopedJoinHandle, scope},
};

use crate::{Gate, num_cpus};

// imports for documentation
#[allow(unused_imports)]
use crate::unordered::Threadpool;

/// An ordered thread pool.
///
/// Retains much of the same functionality as a normal [`Threadpool`]; refer to the documentation there
/// for more general information about the thread pools. It maintains many of the same
/// implementation details and limitations as well, so if not stated otherwise, assume the two pools
/// work in the same way.
///
/// One implementation difference between this pool and the standard unordered [`Threadpool`] is that
/// when passing a worker function to [`OrderedThreadpool::with_num_workers_and_thread_id`], alongside
/// a thread id, the function also receives a monotonically increasing "job index" `usize` value.
/// *This value does not necessarily represent the original index of its corresponding input.* It is simply
/// a monotonically increasing value unique among all inputs given to this threadpool. In the event that
/// this threadpool is used to only process a single iterator, the job index will
/// represent that index. However, subsequent iterators submitted to the pool will not maintain that property;
/// a new pool will have to be constructed if that is desired.
///
/// As this pool is guaranteed to preserve ordering of its inputs, it can be used to perform multithreaded `map`
/// and `filter` operations on iterators passed to it. As such, it has an additional method
/// [`OrderedThreadpool::filter_map`]. This works as you would expect, similar to its [`Iterator`] counterpart,
/// [`Iterator::filter_map`]. Some iterator extensions [`FilterMapMultithread::filter_map_multithread`] and
/// [`FilterMapMultithreadAsync::filter_map_multithread_async`] are available to make this usage pattern even more seamless.
///
/// Of note, in exchange for yielding ordered outputs, results are no longer guaranteed to return immediately once they
/// finish processing; there may be some delay, as results returned sooner than indented are placed inside a buffer, which also uses some
/// additional amount of memory.
pub struct OrderedThreadpool<'scope, 'env, I, O> {
    scope: &'scope Scope<'scope, 'env>,
    #[allow(unused)]
    workers: Vec<ScopedJoinHandle<'scope, ()>>,
    #[allow(unused)]
    filer: ScopedJoinHandle<'scope, ()>,
    in_flight: Gate<usize>,
    producers_active: Gate<usize>,
    work_submission: Sender<(usize, I)>,
    work_reception: Receiver<O>,
    job_index: Arc<Mutex<usize>>,
}

impl<'scope, 'env, I, O> OrderedThreadpool<'scope, 'env, I, O> {
    /// Construct a new [`OrderedThreadpool`].
    ///
    /// Provide a function that workers will use to process jobs,
    /// and a [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    ///
    /// By default, the number of workers spawned is determined by the
    /// result of [`available_parallelism()`].
    /// To specify the specific number of workers to spawn, use
    /// [`OrderedThreadpool::with_num_workers_and_thread_id`] instead.
    pub fn new<F>(f: F, scope: &'scope Scope<'scope, 'env>) -> Self
    where
        F: Fn(I) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        Self::with_num_workers_and_thread_id(move |x, _, _| f(x), scope, num_cpus())
    }

    /// Construct an [`OrderedThreadpool`] with a specific number of workers.
    ///
    /// Provide a function that workers will use to process elements,
    /// and a [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    ///
    /// In addition to the input element `I`, the worker function
    /// also receives two `usize`s. The first represents the index of the
    /// element submitted, and the second represents the thread id, which can be
    /// used to distinguish between individual workers in the pool.
    pub fn with_num_workers_and_thread_id<F>(
        f: F,
        scope: &'scope Scope<'scope, 'env>,
        num_workers: NonZeroUsize,
    ) -> Self
    where
        F: Fn(I, usize, usize) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        let (work_submission, inbox) = channel();
        let (outbox, filer_inbox) = channel();
        let (filer_outbox, work_reception) = channel();
        let in_flight = Gate::new(0usize);
        let job_index = Arc::new(Mutex::new(0));
        let producers_active = Gate::new(0);
        let f = Arc::new(f);
        let workers = (0..num_workers.into())
            .map(|id| {
                let inbox = inbox.clone();
                let outbox = outbox.clone();
                let f = f.clone();
                scope.spawn(move || {
                    for (index, item) in inbox {
                        let result = f(item, index, id);
                        outbox.send((index, result)).unwrap();
                    }
                })
            })
            .collect();
        let filer = {
            let in_flight = in_flight.clone();
            scope.spawn(move || {
                let mut buffer: VecDeque<(usize, Option<O>)> = VecDeque::new();
                let mut least_index = 0;
                for (index, result) in filer_inbox {
                    let insert_index = buffer
                        .binary_search_by_key(&index, |&(i, _)| i)
                        .expect_err("index should never already be present in the buffer");
                    buffer.insert(insert_index, (index, result));
                    while buffer
                        .front()
                        .map(|item| item.0 == least_index)
                        .unwrap_or(false)
                    {
                        // eprintln!("releasing {least_index}");
                        if let Some(item) = buffer.pop_front().unwrap().1 {
                            filer_outbox.send(item).unwrap()
                        }
                        in_flight.update(|x| *x = x.saturating_sub(1));
                        least_index += 1;
                    }
                }
                // ideally buffer should be empty by now, but clear it out just to be safe
                for (_, item) in buffer {
                    if let Some(item) = item {
                        filer_outbox.send(item).unwrap();
                    }
                    in_flight.update(|x| *x = x.saturating_sub(1));
                    // probably not necessary, but just to be *extra* safe and not leave
                    // the pool in an invalid state in some unforseeable edge case,
                    // we increment this
                    least_index += 1;
                }
            })
        };
        Self {
            workers,
            work_submission,
            work_reception,
            in_flight,
            producers_active,
            scope,
            job_index,
            filer,
        }
    }

    /// Synchronously submit a single job to the [`OrderedThreadpool`].
    pub fn submit(&self, input: I) {
        let mut job_index = self.job_index.lock().unwrap();
        self.in_flight.update(|x| *x += 1);
        self.work_submission.send((*job_index, input)).unwrap();
        *job_index += 1;
    }

    /// Synchronously submit many jobs to the [`OrderedThreadpool`] at once from
    /// an iterator or collection.
    pub fn submit_all<T>(&self, iter: T)
    where
        T: IntoIterator<Item = I>,
    {
        iter.into_iter().for_each(|x| self.submit(x));
    }

    /// Block the current thread until a result from the
    /// [`OrderedThreadpool`] is available, and return it.
    pub fn recv(&self) -> O {
        self.work_reception.recv().unwrap()
    }

    /// Check if a result is available from the [`OrderedThreadpool`];
    /// if so, return it. If not, returns `None`.
    pub fn try_recv(&self) -> Option<O> {
        match self.work_reception.try_recv() {
            Ok(val) => Some(val),
            Err(TryRecvError::Empty) => None,
            Err(err) => panic!("{err}"),
        }
    }

    /// Iterate over the currently available results in the [`OrderedThreadpool`].
    ///
    /// Does not consume the pool; it only yields results until
    /// there are no jobs left to process. If more jobs are submitted
    /// to the pool afterwards, those results may be subsequently iterated
    /// over as well.
    ///
    /// It's recommended that you call [`OrderedThreadpool::wait_until_finished`]
    /// before iterating, otherwise the cpu may be stuck in a busy wait while
    /// lingering jobs are still being processed.
    pub fn iter(&self) -> OrderedThreadpoolIter<'_, 'scope, 'env, I, O> {
        OrderedThreadpoolIter(self)
    }

    /// Block until all producers have been exhausted, and all workers
    /// have finished processing all jobs.
    pub fn wait_until_finished(&self) {
        self.producers_active.wait_while(|x| *x > 0);
        self.in_flight.wait_while(|x| *x > 0);
    }

    /// Spawn a new producer thread, which supplies jobs
    /// to the [`OrderedThreadpool`] from the passed iterator asynchronously
    /// on a separate thread.
    ///
    /// Because of the ordered nature of this pool, you probably don't want to
    /// be submitting multiple jobs to the pool simultaneously from separate threads, as the results
    /// will become intermixed nondeterminstically with each other. Nonetheless, even in such
    /// circumstances, the pool guarantees that the original ordering of the individual iterators
    /// will still be preserved, even if the specific interspersing of their
    /// indivdual elements is undefined and nondeterministic.
    pub fn producer<T>(&self, iter: T) -> ScopedJoinHandle<'scope, ()>
    where
        I: Send + Sync + 'scope,
        T: IntoIterator<Item = I> + Send + Sync + 'scope,
    {
        self.producers_active.update(|x| *x += 1);
        let in_flight = self.in_flight.clone();
        let job_index = self.job_index.clone();
        let work_submission = self.work_submission.clone();
        let producers_active = self.producers_active.clone();
        self.scope.spawn(move || {
            for item in iter {
                let mut job_index = job_index.lock().unwrap();
                in_flight.update(|x| *x += 1);
                work_submission.send((*job_index, item)).unwrap();
                *job_index += 1;
            }
            producers_active.update(|x| *x = x.saturating_sub(1));
        })
    }

    /// Spawn a new consumer thread, which consumes and processes
    /// results from the [`OrderedThreadpool`] asynchronously on a
    /// separate thread.
    pub fn consumer<F>(&self, f: F) -> ScopedJoinHandle<'scope, ()>
    where
        O: Send + Sync + 'scope,
        F: Fn(O) + Sync + Send + 'scope,
    {
        let work_reception = self.work_reception.clone();
        self.scope.spawn(move || {
            for item in work_reception {
                f(item);
            }
        })
    }

    /// Use this pool to perform a multithreaded `filter_map` on the
    /// passed iterator.
    ///
    /// Equivalent to calling [`OrderedThreadpool::submit_all`], followed
    /// by [`OrderedThreadpool::iter`].
    pub fn filter_map<'a, T>(&'a self, iter: T) -> OrderedThreadpoolIter<'a, 'scope, 'env, I, O>
    where
        T: IntoIterator<Item = I>,
    {
        self.submit_all(iter);
        self.iter()
    }

    /// Use this pool to perform a multithreaded `filter_map` on the
    /// passed iterator.
    ///
    /// Passes the iterator up to a producer thread, so that results can be
    /// consumed from the pool immediately, even before the iterator finishes being
    /// exhausted.
    pub fn filter_map_async<'a, T>(
        &'a self,
        iter: T,
    ) -> OrderedThreadpoolIter<'a, 'scope, 'env, I, O>
    where
        I: Send + Sync + 'scope,
        T: IntoIterator<Item = I> + Send + Sync + 'scope,
    {
        self.producer(iter);
        self.iter()
    }
}

/// An iterator over an [`OrderedThreadpool`].
///
/// Does not consume the pool; it only yields results until
/// there are no jobs left to process. If more jobs are submitted
/// to the pool afterwards, those results may be subsequently iterated
/// over as well.
///
/// It's recommended that you call [`OrderedThreadpool::wait_until_finished`]
/// before iterating, otherwise the cpu may be stuck in a busy wait while
/// lingering jobs are still being processed.
pub struct OrderedThreadpoolIter<'a, 'scope, 'env, I, O>(&'a OrderedThreadpool<'scope, 'env, I, O>);

impl<'a, 'scope, 'env, I, O> Iterator for OrderedThreadpoolIter<'a, 'scope, 'env, I, O> {
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.work_reception.try_recv() {
                Ok(item) => return Some(item),
                Err(TryRecvError::Empty) => {
                    if self.0.in_flight.check() == 0 && self.0.producers_active.check() == 0 {
                        return None;
                    }
                }
                Err(err) => panic!("{err}"),
            }
            thread::yield_now();
        }
    }
}

impl<'scope, 'env, I, O> Extend<I> for OrderedThreadpool<'scope, 'env, I, O> {
    fn extend<T: IntoIterator<Item = I>>(&mut self, iter: T) {
        self.submit_all(iter);
    }
}

impl<'scope, 'env, I, O> IntoIterator for OrderedThreadpool<'scope, 'env, I, O> {
    type Item = O;

    type IntoIter = IntoIter<O>;

    fn into_iter(self) -> Self::IntoIter {
        drop(self.work_submission);
        self.work_reception.into_iter()
    }
}

/// Extension trait to provide the `filter_map_multithread_async` function
/// for iterators.
pub trait FilterMapMultithreadAsync<'scope> {
    /// Constructs a new
    /// [`OrderedThreadpool`] and uses it to map the iterator
    /// with the passed function in parallel.
    ///
    /// Yields items in send order as they become available, as soon
    /// as possible, preserving the original order of the input.
    ///
    /// Requires a [`Scope`] be passed in order to encapsulate
    /// the lifetime of the threads that the pool spawns, which is
    /// necessary in order to serve results asynchronously as they arrive.
    ///
    /// For a version of this function that does not require a
    /// scope, see [`FilterMapMultithread::filter_map_multithread`]
    fn filter_map_multithread_async<'env, F, O>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> impl Iterator<Item = O> + 'scope
    where
        O: Send + Sync + 'scope,
        Self: Iterator,
        Self::Item: Send + Sync + 'scope,
        F: Fn(Self::Item) -> Option<O> + Send + Sync + 'scope;
}

impl<'scope, T> FilterMapMultithreadAsync<'scope> for T
where
    T: Iterator + Send + Sync + 'scope,
{
    fn filter_map_multithread_async<'env, F, O>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> impl Iterator<Item = O> + 'scope
    where
        O: Send + Sync + 'scope,
        Self: Iterator + 'scope,
        T::Item: Send + Sync + 'scope,
        F: Fn(T::Item) -> Option<O> + Send + Sync + 'scope,
    {
        let pool = OrderedThreadpool::new(f, scope);
        pool.producer(self);
        pool.into_iter()
    }
}

/// Extension trait to provide the `filter_map_multithread` function
/// for iterators.
pub trait FilterMapMultithread {
    /// Constructs a new
    /// [`OrderedThreadpool`] and uses it to map the iterator
    /// with the passed function in parallel.
    ///
    /// Parses and collects the full result into a [`Vec`] before returning.
    ///
    /// Unlike [`FilterMapMultithreadAsync::filter_map_multithread_async`],
    /// calling this function does not require a [`Scope`] to be provided,
    /// as all threads are cleaned up by the time work is done.
    fn filter_map_multithread<F, O>(self, f: F) -> Vec<O>
    where
        O: Send + Sync,
        Self: Iterator,
        Self::Item: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync;
}

impl<T> FilterMapMultithread for T
where
    T: Iterator + Send + Sync,
{
    fn filter_map_multithread<F, O>(self, f: F) -> Vec<O>
    where
        O: Send + Sync,
        Self: Iterator,
        T::Item: Send + Sync,
        F: Fn(T::Item) -> Option<O> + Send + Sync,
    {
        scope(|scope| {
            let pool = OrderedThreadpool::new(f, scope);
            pool.submit_all(self);
            pool.into_iter().collect()
        })
    }
}
