use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{assert_ne, eprintln, panic};

use serde_json::{Map, Value};

pub struct Node {
    pub node_id: String,
    // Unique list of all neighbors/nodes.
    pub node_ids: Vec<String>,

    // While not making Node Sync we are cognizant it will be used in a multithreaded manner.
    // Use AcqRel ordering. `msg_id` must always be `previous + 1`, so Relaxed ordering is out. We
    // don't need to coordinate across any other atomics so SeqCnst shouldn't be needed.
    pub msg_id: AtomicU64,
}

impl Node {
    pub fn new(node_id: &Value, node_ids: &Value) -> Node {
        let node_id = match node_id {
            Value::String(id) => id.clone(),
            _ => panic!("Non-string node_id {}", node_id),
        };
        // Use a HashSet to guarantee each element is unique.
        let node_ids: HashSet<_> = match &node_ids {
            Value::Array(ids) => ids.iter().map(|x| x.as_str().unwrap().to_string()).collect(),
            _ => panic!("Non-string node_id {:?}", node_ids),
        };
        Node { msg_id: AtomicU64::new(0), node_id, node_ids: node_ids.into_iter().collect() }
    }

    pub fn build_message(&self, src: &str, dest: &str, msg_type: &str) -> Map<String, Value> {
        let msg_id = self.msg_id.fetch_add(1, Ordering::AcqRel);
        let msg = serde_json::json!({
            "src": src,
            "dest": dest,
            "body": {
                "msg_id": msg_id,
                "type": msg_type,
            }
        });
        match msg {
            Value::Object(obj) => obj,
            _ => panic!("Invalid message {:?}", msg),
        }
    }

    pub fn build_response(
        &self,
        request: &Map<String, Value>,
        msg_type: &str,
    ) -> Map<String, Value> {
        assert_ne!(&self.node_id, "", "Uninitialized node cannot send responses. {request:?}");

        let mut response =
            self.build_message(&self.node_id, request["src"].as_str().unwrap(), msg_type);
        let Value::Object(response_body) = &mut response["body"] else {
            panic!("Invalid response {:?}", response);
        };
        let Value::Object(request_body) = &request["body"] else {
            panic!("Invalid request {:?}", request);
        };
        response_body.insert("in_reply_to".to_owned(), request_body["msg_id"].clone());
        response
    }
}

// Useful for moving fields instead of copying them.
pub fn take_field<T>(input: &mut Map<String, Value>, name: &str) -> T
where
    T: serde::de::DeserializeOwned,
{
    let serde_json::map::Entry::Occupied(entry) = input.entry(name) else {
        panic!("Invalid field removal {:?}", input)
    };
    serde_json::from_value(entry.remove()).unwrap()
}

// Wait to receive a JSON message and return the parsed version.
pub async fn await_request(stdin: &async_std::io::Stdin) -> Map<String, Value> {
    let mut input = String::new();
    let Ok(_) = stdin.read_line(&mut input).await else {
        panic!("Failed to read from stdin");
    };
    eprintln!("Received {}", input);
    let Ok(request) = serde_json::from_str::<Map<String, Value>>(&input) else {
        panic!("Failed to parse input: {input}");
    };
    request
}

// Awaits an init message, builds a node based on this, responds with init_ok, and returns the node.
pub async fn create_node(stdin: &async_std::io::Stdin) -> Node {
    let request = await_request(stdin).await;
    assert_eq!(request["body"]["type"], "init", "{request:?}");
    eprintln!("Initialized node {}", request["body"]["node_id"]);

    let node = Node::new(&request["body"]["node_id"], &request["body"]["node_ids"]);

    let response = node.build_response(&request, "init_ok");
    let serialized = serde_json::to_string(&response).unwrap();
    println!("{}", serialized);

    node
}
