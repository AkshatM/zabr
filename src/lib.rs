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
use std::fs::{File, read_to_string};
use std::path::Path;
use serde::{Serialize, Deserialize};
use std::net::{SocketAddr,ToSocketAddrs};
use crossbeam::channel::unbounded;
use std::io::{BufRead, BufReader, Result};
use serde_json;

const TRANSACTION_LOG: &str = "transaction_log";
const ACCEPTED_EPOCH_LOG: &str = "epoch_of_last_initiated_leader_election";
const CURRENT_EPOCH_LOG: &str = "epoch_of_last_completed_leader_election";

/// A PeerInterface is how you're expected to start and communicate with your Peer.
///
/// Applications are expected to supply configuration details (specificed in `zabr::Config`) to the PeerInterface before starting it.
/// Internally, PeerInterfaces launch a separate thread in which the Peer runs, and acts as a message passer.
#[derive(Default)]
pub struct PeerInterface {
	peer_handle: Option<crossbeam::channel::Sender<Transaction>>
}

/// This function returns the persisted state of the peer from the local filesystem.
/// All data is pulled from files generated under a directory specified by `path`.
/// If no files exist, default values are returned.
fn recover_from_crash(path: &String) -> (Vec<Transaction>, u64, u64) {

	let root_directory = Path::new(path);

	// File contents: newline separated list of JSON-serialized structs.
	let transaction_log = root_directory.join(TRANSACTION_LOG);
	// File contents: A single number written to a file.
    let accepted_epoch_log = root_directory.join(ACCEPTED_EPOCH_LOG);
    // File contents: A single number written to a file.
    let current_epoch_log = root_directory.join(CURRENT_EPOCH_LOG);

	let accepted_epoch: u64 = match read_to_string(accepted_epoch_log) {
		Ok(s) => s.parse().unwrap_or(0),
		_ => 0,
	};

	let current_epoch: u64 = match read_to_string(current_epoch_log)  {
		Ok(s) => s.parse().unwrap_or(0),
		_ => 0,
	};

	let transaction_log_contents = match read_to_string(transaction_log) {
		Ok(s) => s,
		_ => "".to_string()
	};

    let mut history: Vec<Transaction> = Vec::new();
	for line in transaction_log_contents.split(&"\n") {
		// Should panic if transaction log is corrupted 
		let transaction: Transaction = serde_json::from_str(&line).unwrap();
        history.push(transaction);
	}

	return (history, accepted_epoch, current_epoch);

}

impl PeerInterface {
	/// Launches a Peer in a separate thread. 
	/// Applications must configure their Peer appropriately before starting.
	pub fn start(&mut self, config: Config) -> () {

		if self.peer_handle.is_none() {

            let (crash_history, acc_epoch_at_crash, cur_epoch_at_crash) = recover_from_crash(&config.peer_directory);

			let peer = Peer {
				history: crash_history,
				accepted_epoch: acc_epoch_at_crash,
				current_epoch: cur_epoch_at_crash,
				peer_state: PeerState::Election,
				port_number: config.peer_port,
				remote_peers: config.remote_peers,
				peer_directory: config.peer_directory,
				application_callback: config.application_callback
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
	/// List of node addresses with ports to connect to.
	remote_peers: Vec<String>,
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

impl Default for Config {
	fn default() -> Config {
		return Config {
			remote_peers: Vec::new(),
			// Default port is same as Zookeeper's default port.
			peer_port: 2888,
			// TODO: Support Windows instead of POSIX-compliant machines also.
			peer_directory: "/etc/zabr".to_string(),
			application_callback: |cb| {},
		}
	}
}

/// Zookeeper IDs (zxids) mark every transaction with a unique identifier.
/// This identifier can be compared across new leaders by means of an `epoch`
/// variable, which is incremented on election of a new primary. The `counter`
/// variable, on the other hand, uniqiely identifies transactions within an epoch.
#[derive(Serialize, Deserialize)]
struct ZxID {
	epoch: i64,
	counter: u64
}

/// Transactions are the fundamental change a primary broadcasts to its followers.
/// They contain a `state` variable that contains the new state of the Zookeeper data tree.
/// All data in a Transaction's `state` should be serialized to a stringified format.
// TODO: Extend serialization support beyond just raw string representation.
#[derive(Serialize, Deserialize)]
struct Transaction{
	record: String,
    zxid: ZxID
}
