use std::collections::{HashMap, VecDeque};

use local_sync::mpsc;
use monoio::io::{stream::Stream, CancelableAsyncReadRent, CancelableAsyncWriteRent, Splitable};

use crate::{
    entities::codec::{BackendMessage, BackendMessages, FrontendMessage},
    ops::copy_both::CopyBothReceiver,
    ops::copy_in::CopyInReceiver,
};

pub(crate) mod raw;
pub(crate) mod startup;

pub use raw::RawConnection;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
    CopyBoth(CopyBothReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::bounded::Tx<BackendMessages>,
}

pub struct Response {
    sender: mpsc::bounded::Tx<BackendMessages>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

pub trait Connection<S: CancelableAsyncReadRent + CancelableAsyncWriteRent + Splitable>:
    Stream
{
    fn new(
        stream: S,
        pending_responses: VecDeque<BackendMessage>,
        parameters: HashMap<String, String>,
        receiver: mpsc::unbounded::Rx<Request>,
    ) -> Self;
}
