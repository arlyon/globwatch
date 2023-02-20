use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use futures::{
    channel::mpsc::{Receiver, Sender, UnboundedReceiver},
    future::Either,
    SinkExt, Stream, StreamExt as _,
};
use merge_streams::StreamExt as _;
use notify::{Event, Watcher};
use stop_token::{stream::StreamExt as _, TimedOutError};
use tracing::{event, span, trace, Id, Level, Span};

#[derive(Debug)]
pub struct GlobWatcher<T: Watcher, S>
where
    S: Stream,
{
    watcher: Arc<Mutex<T>>,
    stream: S,

    config: Receiver<GlobConfig>,
}

impl GlobWatcher<notify::RecommendedWatcher, UnboundedReceiver<Event>> {
    #[tracing::instrument]
    pub fn new() -> (Self, GlobSender) {
        let (tx, rx) = futures::channel::mpsc::unbounded();

        let watcher =
            notify::recommended_watcher(move |e: Result<notify::Event, notify::Error>| {
                let span = span!(tracing::Level::TRACE, "watcher");
                let _ = span.enter();
                trace!(parent: &span, "{:?}", e);

                let mut tx = tx.clone();
                futures::executor::block_on(async move { tx.start_send(e.unwrap()) }).unwrap();
            })
            .unwrap();

        let (tconf, rconf) = futures::channel::mpsc::channel(10);

        (
            Self { watcher: Arc::new(Mutex::new(watcher)), stream: rx, config: rconf },
            GlobSender(tconf),
        )
    }
}

impl<T: Watcher, S: Stream + 'static> GlobWatcher<T, S>
where
    S::Item: std::fmt::Debug,
{
    #[tracing::instrument(skip(self))]
    pub fn into_stream(
        self,
        token: stop_token::StopToken,
    ) -> impl Stream<Item = Result<S::Item, TimedOutError>> + Unpin {
        Box::pin(
            self.stream
                .map(Either::Left)
                .merge(self.config.map(Either::Right))
                .filter_map(move |f| {
                    let span = span!(tracing::Level::TRACE, "stream_processor");
                    let _ = span.enter();
                    let watcher = self.watcher.clone();
                    async move {
                        match f {
                            Either::Left(e) => {
                                event!(parent: &span, Level::TRACE, "yielding {:?}", e);
                                Some(e)
                            }
                            Either::Right(GlobConfig::Include(p, id)) => {
                                event!(parent: span.follows_from(id), Level::TRACE, "including {:?}", p);
                                watcher
                                    .lock()
                                    .unwrap()
                                    .watch(&p, notify::RecursiveMode::Recursive)
                                    .expect("Ok");
                                None
                            }
                            Either::Right(GlobConfig::Exclude(p, id)) => {
                                event!(parent: span.follows_from(id), Level::TRACE, "excluding {:?}", p);
                                watcher.lock().unwrap().unwatch(&p).expect("Ok");
                                None
                            }
                        }
                    }
                })
                .timeout_at(token),
        )
    }
}

#[derive(Debug)]
pub enum GlobConfig {
    Include(PathBuf, Option<Id>),
    Exclude(PathBuf, Option<Id>),
}

#[derive(Debug)]
pub struct GlobSender(Sender<GlobConfig>);

impl GlobSender {
    #[tracing::instrument(skip(self))]
    pub async fn include(&mut self, path: PathBuf) {
        trace!("including {:?}", path);
        self.0.send(GlobConfig::Include(path, Span::current().id())).await.unwrap();
    }

    #[tracing::instrument(skip(self))]
    pub async fn exclude(&mut self, path: PathBuf) {
        trace!("excluding {:?}", path);
        self.0.send(GlobConfig::Exclude(path, Span::current().id())).await.unwrap();
    }
}
