use crate::network::{
    eventloop::{Event, EventLoop},
    swarm::ComposedBehaviour,
};
use anyhow::Result;
use libp2p::{request_response::ResponseChannel, Multiaddr, PeerId, Swarm};
use std::collections::HashSet;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    /// Initialize a client with an event receiver and event loop.
    pub async fn new(
        swarm: Swarm<ComposedBehaviour>,
    ) -> Result<(Self, mpsc::Receiver<Event>, EventLoop)> {
        let (command_sender, command_receiver) = mpsc::channel(1);
        let (event_sender, event_receiver) = mpsc::channel(1);

        Ok((
            Client {
                sender: command_sender,
            },
            event_receiver,
            EventLoop::new(swarm, command_receiver, event_sender),
        ))
    }

    /// Listen for incoming connections on the given address.
    pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await?;
        receiver.await?
    }

    /// Dial the given peer at the given address.
    pub async fn dial(&mut self, peer_id: PeerId, peer_addr: Multiaddr) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial {
                peer_id,
                peer_addr,
                sender,
            })
            .await?;
        receiver.await?
    }

    /// Advertise the local node as the provider of the given file on the DHT.
    pub async fn start_providing(&mut self, file_name: String) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartProviding { file_name, sender })
            .await?;
        receiver.await?
    }

    /// Find the providers for the given file on the DHT.
    pub async fn get_providers(&mut self, file_name: String) -> Result<HashSet<PeerId>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetProviders { file_name, sender })
            .await?;
        receiver.await?
    }

    /// Request the content of the given file from the given peer.
    pub async fn request_file(&mut self, peer: PeerId, file_name: String) -> Result<Vec<u8>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestFile {
                file_name,
                peer,
                sender,
            })
            .await?;
        receiver.await?
    }

    /// Respond with the provided file content to the given request.
    pub async fn respond_file(
        &mut self,
        file: Vec<u8>,
        channel: ResponseChannel<FileResponse>,
    ) -> Result<()> {
        self.sender
            .send(Command::RespondFile { file, channel })
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRequest(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileResponse(pub(crate) Vec<u8>);

#[derive(Debug)]
pub enum Command {
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<()>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<()>>,
    },
    StartProviding {
        file_name: String,
        sender: oneshot::Sender<Result<()>>,
    },
    GetProviders {
        file_name: String,
        sender: oneshot::Sender<Result<HashSet<PeerId>>>,
    },
    RequestFile {
        file_name: String,
        peer: PeerId,
        sender: oneshot::Sender<Result<Vec<u8>>>,
    },
    RespondFile {
        file: Vec<u8>,
        channel: ResponseChannel<FileResponse>,
    },
}
