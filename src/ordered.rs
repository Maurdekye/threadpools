//! Ordered thread pool implementation.
//!
//! See [`OrderedThreadpool`] for more detailed documentation.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        mpmc::{IntoIter, Receiver, Sender},
        mpsc::TryRecvError,
    },
    thread::{self, Scope, ScopedJoinHandle, scope},
};

use crate::{Gate, GenericThreadpool, channel, num_cpus};

// imports for documentation
#[allow(unused_imports)]
use crate::unordered::{Threadpool, ThreadpoolBuilder};
#[allow(unused_imports)]
use std::thread::available_parallelism;

/// An ordered thread pool.
///
/// Yields results in the same order they are submitted to the pool.
///
/// Retains much of the same functionality as a normal [`Threadpool`]; refer to the documentation there
/// for more general information about the thread pools. It maintains many of the same
/// implementation details and limitations as well, so if not stated otherwise, assume the two pools
/// work in the same way.
///
/// Also like the [`Threadpool`], the [`OrderedThreadpool`] has its own [`OrderedThreadpoolBuilder`],
/// which allows you to configure all the same options as you would be able to with a [`ThreadpoolBuilder`].
///
/// One implementation difference between this pool and the standard unordered [`Threadpool`] is that
/// when passing a worker function to [`OrderedThreadpoolBuilder::build`], alongside
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
/// [`FilterMapAsync::filter_map_async`] are available to make this usage pattern even more seamless.
///
/// Of note, in exchange for yielding ordered outputs, results are no longer guaranteed to return immediately once they
/// finish processing; there may be some delay, as results returned sooner than intended are placed inside a buffer, which also uses some
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
    /// result of [`available_parallelism`].
    /// To specify the specific number of workers to spawn, and configure
    /// several additional other custom properties including an initializer
    /// and thread blocking properties, construct your pool via an
    /// [`OrderedThreadpoolBuilder`] instead.
    pub fn new<F>(f: F, scope: &'scope Scope<'scope, 'env>) -> Self
    where
        F: Fn(I) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        OrderedThreadpoolBuilder::new().build(move |x, _| f(x), scope)
    }

    fn new_inner<F, N, M>(
        f: F,
        initializer: N,
        scope: &'scope Scope<'scope, 'env>,
        num_workers: NonZeroUsize,
        blocking_submission: bool,
        blocking_workers: bool,
    ) -> Self
    where
        F: Fn(I, (&mut M, usize, usize)) -> Option<O> + Send + Sync + 'scope,
        N: Fn(usize) -> M + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        let (work_submission, inbox) = channel(blocking_submission);
        let (outbox, filer_inbox) = channel(blocking_workers);
        let (filer_outbox, work_reception) = channel(blocking_workers);
        let in_flight = Gate::new(0usize);
        let job_index = Arc::new(Mutex::new(0));
        let producers_active = Gate::new(0);
        let f = Arc::new(f);
        let initializer = Arc::new(initializer);
        let workers = (0..num_workers.into())
            .map(|id| {
                let inbox = inbox.clone();
                let outbox = outbox.clone();
                let f = f.clone();
                let initializer = initializer.clone();
                scope.spawn(move || {
                    let mut thread_state = initializer(id);
                    for (index, item) in inbox {
                        let result = f(item, (&mut thread_state, id, index));
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
                    if index == least_index {
                        if let Some(item) = result {
                            filer_outbox.send(item).unwrap()
                        }
                        in_flight.update(|x| *x = x.saturating_sub(1));
                        least_index += 1;
                        while buffer
                            .front()
                            .map(|item| item.0 == least_index)
                            .unwrap_or(false)
                        {
                            if let Some(item) = buffer.pop_front().unwrap().1 {
                                filer_outbox.send(item).unwrap()
                            }
                            in_flight.update(|x| *x = x.saturating_sub(1));
                            least_index += 1;
                        }
                    } else {
                        let insert_index = buffer
                            .binary_search_by_key(&index, |&(i, _)| i)
                            .expect_err("index should never already be present in the buffer");
                        buffer.insert(insert_index, (index, result));
                    }
                }
                // ideally buffer should be empty by now, but clear it out just to be safe
                for (_, item) in buffer {
                    if let Some(item) = item {
                        filer_outbox.send(item).unwrap();
                    }
                    in_flight.update(|x| *x = x.saturating_sub(1));
                    // probably not necessary, but just to be *extra* safe and not leave
                    // the pool in an invalid state in some unforeseeable edge case,
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

    /// Use this pool to perform a multithreaded `filter_map` on the
    /// passed iterator.
    ///
    /// Equivalent to calling [`OrderedThreadpool::submit_all`], followed
    /// by [`OrderedThreadpool::iter`].
    pub fn filter_map<'a, T>(&'a self, iter: T) -> OrderedThreadpoolIter<'a, 'scope, 'env, I, O>
    where
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
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
        O: Send + Sync + 'scope,
        T: IntoIterator<Item = I> + Send + Sync + 'scope,
    {
        self.producer(iter);
        self.iter()
    }
}

impl<'scope, 'env, I, O> GenericThreadpool<'scope, I, O> for OrderedThreadpool<'scope, 'env, I, O>
where
    I: Send + Sync + 'scope,
    O: Send + Sync + 'scope,
{
    type Iter<'a>
        = OrderedThreadpoolIter<'a, 'scope, 'env, I, O>
    where
        Self: 'a;

    type JoinHandle = ScopedJoinHandle<'scope, ()>;

    fn submit(&self, input: I) {
        let mut job_index = self.job_index.lock().unwrap();
        self.in_flight.update(|x| *x += 1);
        self.work_submission.send((*job_index, input)).unwrap();
        *job_index += 1;
    }

    fn recv(&self) -> O {
        self.work_reception.recv().unwrap()
    }

    fn try_recv(&self) -> Option<O> {
        match self.work_reception.try_recv() {
            Ok(val) => Some(val),
            Err(TryRecvError::Empty) => None,
            Err(err) => panic!("{err}"),
        }
    }

    fn iter(&self) -> OrderedThreadpoolIter<'_, 'scope, 'env, I, O> {
        OrderedThreadpoolIter(self)
    }

    fn wait_until_finished(&self) {
        self.producers_active.wait_while(|x| *x > 0);
        self.in_flight.wait_while(|x| *x > 0);
    }

    /// Spawn a new producer thread, which supplies jobs
    /// to the pool from the passed iterator asynchronously
    /// on a separate thread.
    ///
    /// Because of the ordered nature of this pool, you probably don't want to
    /// be submitting multiple jobs to the pool simultaneously from separate threads, as the results
    /// will become intermixed nondeterministically with each other. Nonetheless, even in such
    /// circumstances, the pool guarantees that the original ordering of the individual iterators
    /// will still be preserved, even if the specific interspersing of their
    /// individual elements is undefined and nondeterministic.
    fn producer<T>(&self, iter: T) -> ScopedJoinHandle<'scope, ()>
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

    fn consumer<F>(&self, f: F) -> ScopedJoinHandle<'scope, ()>
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
}

/// A builder for an [`OrderedThreadpool`].
///
/// Allows the custom configuration of several pool properties.
///
/// Refer to individual method implementations for more details.
///
/// ```rust
/// use std::thread::scope;
/// use std::num::NonZeroUsize;
/// use threadpools::*;
///
/// scope(|s| {
///     let pool = OrderedThreadpoolBuilder::new()
///         .num_workers(NonZeroUsize::new(4).unwrap())
///         .blocking_submission()
///         .build(|x: i32, _| Some(x * x), s);
///
///     pool.submit_all(1..=4);
///
///     pool.wait_until_finished();
///
///     let mut results: Vec<_> = pool.iter().collect();
///
///     assert_eq!(results, vec![1, 4, 9, 16]);
/// });
/// ```
#[derive(Clone, Debug)]
pub struct OrderedThreadpoolBuilder {
    num_workers: NonZeroUsize,
    blocking_submission: bool,
    blocking_workers: bool,
}

impl OrderedThreadpoolBuilder {
    /// Construct a new builder with default properties.
    ///
    /// Calling [`OrderedThreadpoolBuilder::build`] immediately on this
    /// constructs the same threadpool as calling [`OrderedThreadpool::new`].
    pub fn new() -> OrderedThreadpoolBuilder {
        OrderedThreadpoolBuilder {
            num_workers: num_cpus(),
            blocking_submission: false,
            blocking_workers: false,
        }
    }

    /// Set an initializer function.
    ///
    /// This function is run once per worker thread, and is used to
    /// initialize any thread-local state that the worker function may need. It is
    /// passed the thread id as an argument, and should return a value representing the
    /// thread-local state.
    pub fn initializer<I, M>(self, initializer: I) -> InitializedOrderedThreadpoolBuilder<I>
    where
        I: Fn(usize) -> M,
    {
        let OrderedThreadpoolBuilder {
            num_workers,
            blocking_submission,
            blocking_workers,
        } = self;
        InitializedOrderedThreadpoolBuilder {
            initializer,
            num_workers,
            blocking_submission,
            blocking_workers,
        }
    }

    /// Set the specific number of worker threads to spawn.
    ///
    /// By default, the number of pool workers is the same as the
    /// value returned by [`available_parallelism`].
    pub fn num_workers(self, num_workers: NonZeroUsize) -> Self {
        Self {
            num_workers,
            ..self
        }
    }

    /// Enable blocking job submission.
    ///
    /// By default, jobs are be submitted instantly
    /// without blocking, and placed in a work queue.
    /// If enabled, newly submitted jobs will block until
    /// a worker is available to process them.
    pub fn blocking_submission(self) -> Self {
        Self {
            blocking_submission: true,
            ..self
        }
    }

    /// Enable blocking work result submission.
    ///
    /// By default, when workers finish processing a job,
    /// the result is placed in a result queue, and the worker
    /// may continue picking up new jobs immediately. If this is
    /// enabled, workers will block upon finishing their current
    /// job until the result is retrieved from the pool.
    pub fn blocking_workers(self) -> Self {
        Self {
            blocking_workers: true,
            ..self
        }
    }

    /// Construct a new [`OrderedThreadpool`] from the builder.
    ///
    /// ## Args
    ///
    /// * `f` - The function that workers will use to process jobs. It takes
    /// two arguments: the input job `I`, and a tuple of thread metadata.
    /// The metadata tuple consists of two values itself: a `usize` thread id,
    /// and another `usize` for the job index.
    ///
    /// `scope` - The [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    pub fn build<'scope, 'env, F, I, O>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> OrderedThreadpool<'scope, 'env, I, O>
    where
        F: Fn(I, (usize, usize)) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        OrderedThreadpool::new_inner(
            move |i, (_, id, jid)| f(i, (id, jid)),
            |_| (),
            scope,
            self.num_workers,
            self.blocking_submission,
            self.blocking_workers,
        )
    }
}

/// A builder for an [`OrderedThreadpool`] with an initializer configured.
///
/// Allows the custom configuration of several pool properties.
///
/// Refer to individual method implementations for more details.
/// 
/// ```rust
/// use std::thread::scope;
/// use std::num::NonZeroUsize;
/// use threadpools::*;
///
/// scope(|s| {
///     let pool = OrderedThreadpoolBuilder::new()
///         .num_workers(NonZeroUsize::new(4).unwrap())
///         .blocking_submission()
///         .build(|x: i32, _| Some(x * x), s);
///
///     pool.submit_all(1..=4);
///
///     pool.wait_until_finished();
///
///     let mut results: Vec<_> = pool.iter().collect();
///
///     assert_eq!(results, vec![1, 4, 9, 16]);
/// });
/// ```
pub struct InitializedOrderedThreadpoolBuilder<N> {
    initializer: N,
    num_workers: NonZeroUsize,
    blocking_submission: bool,
    blocking_workers: bool,
}

impl<N> InitializedOrderedThreadpoolBuilder<N> {
    /// Set a new initializer function.
    ///
    /// This function is run once per worker thread, and is used to
    /// initialize any thread-local state that the worker function may need. It is
    /// passed the thread id as an argument, and should return a value representing the
    /// thread-local state.
    ///
    /// *(why would you call this again? you already set an initializer...)*
    pub fn initializer<I, M>(self, initializer: I) -> InitializedOrderedThreadpoolBuilder<I>
    where
        I: Fn(usize) -> M,
    {
        InitializedOrderedThreadpoolBuilder {
            initializer,
            ..self
        }
    }

    /// Set the specific number of worker threads to spawn.
    ///
    /// By default, the number of pool workers is the same as the
    /// value returned by [`available_parallelism`].
    pub fn num_workers(self, num_workers: NonZeroUsize) -> Self {
        Self {
            num_workers,
            ..self
        }
    }

    /// Enable blocking job submission.
    ///
    /// By default, jobs are be submitted instantly
    /// without blocking, and placed in a work queue.
    /// If enabled, newly submitted jobs will block until
    /// a worker is available to process them.
    pub fn blocking_submission(self) -> Self {
        Self {
            blocking_submission: true,
            ..self
        }
    }

    /// Enable blocking work result submission.
    ///
    /// By default, when workers finish processing a job,
    /// the result is placed in a result queue, and the worker
    /// may continue picking up new jobs immediately. If this is
    /// enabled, workers will block upon finishing their current
    /// job until the result is retrieved from the pool.
    pub fn blocking_workers(self) -> Self {
        Self {
            blocking_workers: true,
            ..self
        }
    }

    /// Construct a new [`OrderedThreadpool`] from the builder.
    ///
    /// ## Args
    ///
    /// * `f` - The function that workers will use to process jobs. It takes
    /// two arguments: the input job `I`, and a tuple of thread metadata.
    /// The metadata tuple consists of three values itself: a mutable reference
    /// to the value returned by your initializer, a `usize` thread id,
    /// and another `usize` for the job index.
    ///
    /// `scope` - The [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    pub fn build<'scope, 'env, F, I, O, M>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> OrderedThreadpool<'scope, 'env, I, O>
    where
        F: Fn(I, (&mut M, usize, usize)) -> Option<O> + Send + Sync + 'scope,
        N: Fn(usize) -> M + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        OrderedThreadpool::new_inner(
            f,
            self.initializer,
            scope,
            self.num_workers,
            self.blocking_submission,
            self.blocking_workers,
        )
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

impl<'scope, 'env, I, O> Extend<I> for OrderedThreadpool<'scope, 'env, I, O>
where
    I: Send + Sync + 'scope,
    O: Send + Sync + 'scope,
{
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

/// Extension trait to provide the `filter_map_async` function
/// for iterators.
pub trait FilterMapAsync<'scope> {
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
    fn filter_map_async<'env, F, O>(
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

impl<'scope, T> FilterMapAsync<'scope> for T
where
    T: Iterator + Send + Sync + 'scope,
{
    fn filter_map_async<'env, F, O>(
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
    /// Unlike [`FilterMapAsync::filter_map_async`],
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
