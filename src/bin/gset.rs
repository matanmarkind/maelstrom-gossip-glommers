use std::collections::{HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::{panic};

use parking_lot::RwLock;
use serde_json::{Map, Value};


struct Node {
    inner: maelstrom_gossip_glommers::Node,
    messages: HashSet<u64>,
}

impl Node {
    fn new(inner: maelstrom_gossip_glommers::Node) -> Self {
        Self { inner, messages: HashSet::new() }
    }

    fn handle_add(&mut self, mut request: Map<String, Value>) {
        // Build response before taking fields from `request`.
        let response = self.inner.build_response(&request, "add_ok");
        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);

        let mut body: Map<String, Value> =
            maelstrom_gossip_glommers::take_field(&mut request, "body");
        let element: u64 = maelstrom_gossip_glommers::take_field(&mut body, "element");
        self.messages.insert(element);
    }

    fn handle_read(&self, request: Map<String, Value>) {
        let mut response = self.inner.build_response(&request, "read_ok");
        response["body"]["value"] = serde_json::json!(&self.messages);
        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }

    fn handle_replicate(&mut self, mut request: Map<String, Value>) {
        let mut body: Map<String, Value> =
            maelstrom_gossip_glommers::take_field(&mut request, "body");
        let value: HashSet<u64> = maelstrom_gossip_glommers::take_field(&mut body, "value");
        self.messages.extend(value.into_iter());
    }

    fn send_replication(&self) {
        for n in self.inner.node_ids.iter().filter(|&n| *n != self.inner.node_id) {
            let mut msg = self.inner.build_message(&self.inner.node_id, n, "replicate");
            msg["body"]["value"] = serde_json::json!(&self.messages);
            let serialized = serde_json::to_string(&msg).unwrap();
            println!("{}", serialized);
        }
    }
}

fn spawn_periodic_replication(node: Arc<RwLock<Node>>) {
    tokio::spawn(async move {
        loop {
            node.read().send_replication();
            tokio::time::sleep(Duration::from_secs(5)).await;
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
