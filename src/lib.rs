use std::fs::File;
use std::vec::Vec;
use std::any::Any;
use std::path::Path;
use std::net::{SocketAddr};
use actix::{Addr, Arbiter}

//! An implementation of Zookeeper Atomic Broadcast protocol (or ZAB) in Rust.
//!
//! The `zabr` library replicates data across multiple nodes in a cluster, provided all nodes are using `zabr`, are aware of each other,
//! and utilise the same `zabr` configuration.
//!
//! Applications can invoke zabr to handle disseminating data updates to all nodes using the `Peer` struct. Applications are responsible
//! for managing and handling state locally in a node. For example, in Zookeeper, read calls return lookups from node-local data, 
//! while write calls are propagated in a serializable fashion across all nodes and return success. Both read and write calls against 
//! the data, as well as data storage, would be handled by the application - write call propagation, on the other hand, would be 
//! handled by `zabr`. 
//! 
//! Internally, `zabr` elects one node in this cluster to be a leader, and forwards all writes to it from any follower that submits writes.
//! Leaders use a variant of two-phase commit to ensure a quorum of all followers accept the updated write. Forwarded writes are collected
//! and processed in FIFO order on the primary. `zabr` effectively maintains a consistent replicated log containing all write calls 
//! successfully replicated to the system on all nodes. Applications can register handlers that receive new entries written to this log. 
//! 
//! The `zabr` log is persisted to disk - it is thus possible for applications to recover simply by reading the transaction log, and 
//! rejoin the cluster.

/// A PeerInterface is how you're expected to start and communicate with your Peer.
///
/// Applications are expected to supply configuration details (specificed in `zabr::Config`) to the PeerInterface before starting it.
/// Internally, PeerInterfaces launch a separate thread in which the Peer runs, and acts as a message passer.
#[derive(Default)]
pub struct PeerInterface {
	peerAddress: Some(Addr),
};

impl PeerInterface {
	
	/// Launches a Peer in a separate thread. 
	/// Applications must configure their Peer appropriately before starting.
	pub fn start(&mut self, config: Config) -> () {

		// TODO: Implement reading from Config.
		let peer = Peer {
			history: Vec<Transaction>::new(),
			acceptedEpoch: 0,
			currentEpoch: 0,
			peerState: PeerState::Election,
			portNumber: u16,
			remotePeerConnections: Vec<SocketAddr>::new(),
			applicationCallback = config.applicationCallback,
		};

		self.peerAddress = Arbiter::new("PeerThread").start(|_| peer::start);
	}

	pub fn submit(&self, message: Any) -> Result<()> {
		self.peerAddress.try_send(Write(message))?;
	}
}

/// Peers are `zabr`'s workhorses. There should only ever be one Peer per node in an application. 
///
/// Peers handle leader election, write forwarding to the leader, and write broadcast if the Peer on a node is a leader. Peers recover from
/// state crashes, maintain a consistent log applications can register listeners for, and persist stateful information to disk for recovery.
struct Peer {
	/// A transaction log of all the ZXIDs that have been processed, sorted chronologically by ZxId.
	history: Vec<Transaction>,
	/// The epoch number the last leader election was initiated with. Obviously can be ahead of currentEpoch once a leader election 
	/// is initiated after the last election is completed. currentEpoch and acceptedEpoch should both converge eventually.
    acceptedEpoch: u64,
    /// The epoch number that the last leader election was completed with.
    currentEpoch: u64,
	/// What state a peer is in at runtime. Is always initialized with state peerState::Election at system start, but will change
	/// dynamically over system runtime. 
	peerState: PeerState,
	/// The port number Peer will bind to for incoming messages.
	portNumber: u16,
	/// Active socket connections maintained against all the other nodes.
	remotePeerConnections: Vec<SocketAddr>,
	/// Data + configuration directory managed by this peer, used for recovery purposes.
	peerDirectory: Path,
	/// Applications can register a callback that handles successful replicated updates to the write log. 
	applicationCallback: fn(cb: CallbackMessage) -> Any
}

enum PeerState {
	Election,
	Follower,
	Leader
}

impl Peer {

	/// Binds to a port. Sets up handlers to process messages on port, and broadcasts messages to the world.
	/// Persists to disk after every successful transaction. Basically gets the show on the road. 
	// TODO: Fill this out
    fn start(&self) {

	}

}

/// Config struct is a struct that the application can use to configure Peers.
/// Configs take the following options:
///
/// 1. A list of DNS/IP addresses for all other peers specified as strings. 
///	   Peers will perform DNS resolution and establish connections. 
///	   Invalid entries will cause the Peer to panic. 
/// 2. A port number for the Peer to bind to. Occupied ports will cause the Peer to panic.
/// 3. A directory handle for peers to store persistent data in. Directory should have
///	   full read/write permissions by the application, else peer will panic.
/// 4. A callback that lets the application do something when `zabr` successfully replicates
///    a pending update.
struct Config {
	remotePeers: Vec<String>,
	peerPort: u16,
	peerDirectory: Path,
	applicationCallback: fn(cb: CallbackMessage) -> Any
}

/// Zookeeper IDs (zxids) mark every transaction with a unique identifier.
/// This identifier can be compared across new leaders by means of an `epoch`
/// variable, which is incremented on election of a new primary. The `counter`
/// variable, on the other hand, uniqiely identifies transactions within an epoch.
struct ZxID {
	epoch: i64,
	counter: u64
}

/// Transactions are the fundamental change a primary broadcasts to its followers.
/// They contain a `state` variable that contains the new state of the Zookeeper data tree.
struct Transaction {
	state: Any,
    zxid: ZxID
}

struct Write {
	message: Any,
};

impl Message for Write {
	type Result = ();
}

impl Handler<Write> for Peer {
	type Result = ();

	/// Handle write messages.
	// TODO: Fill this out.
	fn handle(&mut self, msg: Write, ctx: &mut Context<Self>) -> Self::Result {
	
	}
}
