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

use std::vec::Vec;
use std::any::Any;
use std::net::{SocketAddr};
use crossbeam::channel::unbounded;

/// A PeerInterface is how you're expected to start and communicate with your Peer.
///
/// Applications are expected to supply configuration details (specificed in `zabr::Config`) to the PeerInterface before starting it.
/// Internally, PeerInterfaces launch a separate thread in which the Peer runs, and acts as a message passer.
#[derive(Default)]
pub struct PeerInterface {
	peer_handle: Option<crossbeam::channel::Sender<Transaction>>
}

impl PeerInterface {
	/// Launches a Peer in a separate thread. 
	/// Applications must configure their Peer appropriately before starting.
	pub fn start(&mut self, config: Config) -> () {

		if self.peer_handle.is_none() {
			// TODO: Read from config and data record
			let peer = Peer {
				history: Vec::new(),
				accepted_epoch: 0,
				current_epoch: 0,
				peer_state: PeerState::Election,
				port_number: 2888, // Zookeeper default port to converse with other hosts
				remote_peer_connections: Vec::new(),
				peer_directory: "/etc/zabr".to_string(),
				application_callback: |cb| {}
			};

			let (sender, receiver) = unbounded();

			peer.start(receiver);

			self.peer_handle = Some(sender);
		}
	}

	// TODO: Implement this.
	pub fn submit(&self) {	
	
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
    accepted_epoch: u64,
    /// The epoch number that the last leader election was completed with.
    current_epoch: u64,
	/// What state a peer is in at runtime. Is always initialized with state peerState::Election at system start, but will change
	/// dynamically over system runtime. 
	peer_state: PeerState,
	/// The port number Peer will bind to for incoming messages.
	port_number: u16,
	/// Active socket connections maintained against all the other nodes.
	remote_peer_connections: Vec<SocketAddr>,
	/// Data + configuration directory managed by this peer, used for recovery purposes.
	peer_directory: String,
	/// Applications can register a callback that handles successful replicated updates to the write log. 
	application_callback: fn(cb: CallbackMessage) -> ()
}

enum PeerState {
	Election,
	Follower,
	Leader
}

/// Applications receive a CallbackMessage in their registered callback.
struct CallbackMessage {
	message: Box<dyn Any>
}

impl Peer {

	/// Binds to a port. Sets up handlers to process messages on port, and broadcasts messages to the world.
	/// Persists to disk after every successful transaction. Basically gets the show on the road. 
	// TODO: Fill this out
    fn start(&self, receiver: crossbeam::channel::Receiver<Transaction>) {

		
		
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
#[derive(Clone)]
pub struct Config {
	remote_peers: Vec<String>,
	peer_port: u16,
	peer_directory: String,
	application_callback: fn(cb: CallbackMessage) -> ()
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
	state: Box <dyn Any>,
    zxid: ZxID
}
