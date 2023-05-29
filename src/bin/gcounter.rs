use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::panic;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde_json::{Map, Value};

struct Node {
    inner: maelstrom_gossip_glommers::Node,
    node_to_count: HashMap<String, i64>,
}

impl Node {
    fn new(inner: maelstrom_gossip_glommers::Node) -> Self {
        let mut node_to_count = HashMap::new();
        node_to_count.insert(inner.node_id.clone(), 0_i64);
        Self { inner, node_to_count }
    }

    fn handle_add(&mut self, mut request: Map<String, Value>) {
        // Build response before taking fields from `request`.
        let response = self.inner.build_response(&request, "add_ok");
        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);

        let mut body: Map<String, Value> =
            maelstrom_gossip_glommers::take_field(&mut request, "body");
        let delta: i64 = maelstrom_gossip_glommers::take_field(&mut body, "delta");
        let entry = self.node_to_count.get_mut(&self.inner.node_id).unwrap();
        *entry += delta;
    }

    fn handle_read(&self, request: Map<String, Value>) {
        let mut response = self.inner.build_response(&request, "read_ok");
        let sum: i64 = self.node_to_count.values().sum();
        response["body"]["value"] = serde_json::json!(sum);
        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }

    fn handle_replicate(&mut self, mut request: Map<String, Value>) {
        let mut body: Map<String, Value> =
            maelstrom_gossip_glommers::take_field(&mut request, "body");
        let value: Map<String, Value> = maelstrom_gossip_glommers::take_field(&mut body, "value");

        // Record the highest value for each node.
        for (k, v) in value.into_iter().filter(|(k, _v)| *k != self.inner.node_id) {
            match self.node_to_count.entry(k) {
                Entry::Occupied(mut entry) => {
                    let v = v.as_i64().unwrap();
                    if *entry.get() < v {
                        entry.insert(v);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(v.as_i64().unwrap());
                }
            }
        }
    }

    fn send_replication(&self) {
        let counters = serde_json::json!(&self.node_to_count);
        for n in self.inner.node_ids.iter().filter(|&n| *n != self.inner.node_id) {
            let mut msg = self.inner.build_message(&self.inner.node_id, n, "replicate");
            msg["body"]["value"] = counters.clone();
            let serialized = serde_json::to_string(&msg).unwrap();
            println!("{}", serialized);
        }
    }
}

fn spawn_periodic_replication(node: Arc<RwLock<Node>>) {
    tokio::spawn(async move {
        loop {
            node.read().send_replication();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

fn spawn_handler(node: Arc<RwLock<Node>>, request: Map<String, Value>) {
    tokio::spawn(async move {
        let Value::String(msg_type) = &request["body"]["type"] else {
            panic!("Invalid msg type encoding");
        };

        match msg_type.as_str() {
            "init" => panic!("Already initialized node: {:?}", request),
            "add" => node.write().handle_add(request),
            "read" => node.read().handle_read(request),
            "replicate" => node.write().handle_replicate(request),
            _ => panic!("Unknown msg type {:?}", request),
        };
    });
}

#[tokio::main]
async fn main() {
    let stdin = async_std::io::stdin();
    let node = Node::new(maelstrom_gossip_glommers::create_node(&stdin).await);
    let node = Arc::new(RwLock::new(node));

    spawn_periodic_replication(Arc::clone(&node));

    // Main loop.
    loop {
        let request = maelstrom_gossip_glommers::await_request(&stdin).await;
        spawn_handler(Arc::clone(&node), request);
    }
}
