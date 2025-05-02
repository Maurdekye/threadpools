#![feature(mpmc_channel)]
use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Condvar, Mutex,
        mpmc::{Receiver, Sender, channel},
        mpsc::TryRecvError,
    },
    thread::{Scope, ScopedJoinHandle, available_parallelism},
};

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
}

pub struct Threadpool<'scope, 'b, I, O> {
    scope: &'scope Scope<'scope, 'b>,
    #[allow(unused)]
    workers: Vec<ScopedJoinHandle<'scope, ()>>,
    work_submission: Sender<I>,
    work_reception: Receiver<O>,
    in_flight: Gate<usize>,
}

impl<'scope, 'b, I, O> Threadpool<'scope, 'b, I, O> {
    pub fn new<F>(f: F, scope: &'scope Scope<'scope, 'b>) -> Self
    where
        F: Fn(usize, I) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        Self::new_with_work_capacity(
            f,
            scope,
            available_parallelism().unwrap_or(NonZeroUsize::MIN),
        )
    }

    pub fn new_with_work_capacity<F>(
        f: F,
        scope: &'scope Scope<'scope, 'b>,
        num_workers: NonZeroUsize,
    ) -> Self
    where
        F: Fn(usize, I) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        let (work_submission, inbox) = channel();
        let (outbox, work_reception) = channel();
        let in_flight = Gate::new(0usize);
        let f = Arc::new(f);
        let workers = (0..num_workers.into())
            .map(|id| {
                let inbox = inbox.clone();
                let outbox = outbox.clone();
                let in_flight = in_flight.clone();
                let f = f.clone();
                scope.spawn(move || {
                    for item in inbox {
                        let result = f(id, item);
                        if let Some(result) = result {
                            outbox.send(result).unwrap();
                        }
                        in_flight.update(|x| *x = x.saturating_sub(1));
                    }
                })
            })
            .collect();
        Self {
            workers,
            work_submission,
            work_reception,
            in_flight,
            scope,
        }
    }

    pub fn submit(&self, input: I) {
        self.in_flight.update(|x| *x += 1);
        self.work_submission.send(input).unwrap()
    }

    pub fn recv(&self) -> O {
        self.work_reception.recv().unwrap()
    }

    pub fn try_recv(&self) -> Option<O> {
        match self.work_reception.try_recv() {
            Ok(val) => Some(val),
            Err(TryRecvError::Empty) => None,
            _ => panic!(),
        }
    }

    pub fn iter(&self) -> ThreadpoolIter<'_, 'scope, 'b, I, O> {
        ThreadpoolIter(self)
    }

    pub fn into_iter(self) -> impl Iterator<Item = O> {
        drop(self.work_submission);
        self.work_reception.into_iter()
    }

    pub fn wait_until_finished(&self) {
        self.in_flight.wait_while(|x| *x > 0);
    }

    pub fn consumer(&self, f: impl Fn(O) + 'scope + Sync + Send)
    where
        O: Send + Sync + 'scope,
    {
        let work_reception = self.work_reception.clone();
        self.scope.spawn(move || {
            for item in work_reception {
                f(item);
            }
        });
    }
}

pub struct ThreadpoolIter<'a, 'scope, 'b, I, O>(&'a Threadpool<'scope, 'b, I, O>);

impl<'a, 'scope, 'b, I, O> Iterator for ThreadpoolIter<'a, 'scope, 'b, I, O> {
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.work_reception.try_recv().ok()
    }
}

impl<'scope, 'b, I, O> Extend<I> for Threadpool<'scope, 'b, I, O> {
    fn extend<T: IntoIterator<Item = I>>(&mut self, iter: T) {
        for item in iter {
            self.submit(item);
        }
    }
}
