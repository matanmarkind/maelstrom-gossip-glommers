use std::collections::HashSet;
use std::{eprintln, panic};

use serde::de::DeserializeOwned;
use serde_json::{from_value, json, Map, Value};

struct MessageBuilder {
    msg_id: i32,
}

impl MessageBuilder {
    fn new() -> Self {
        Self { msg_id: 0 }
    }
    fn build_message(&mut self, src: &str, dest: &str, msg_type: &str) -> Map<String, Value> {
        let msg_id = self.msg_id;
        self.msg_id += 1;
        let msg = json!({
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
}

struct Node {
    node_id: String,
    node_ids: Vec<String>,
    neighbors: Vec<String>,
    messages: HashSet<u64>,
    msg_builder: MessageBuilder,
}

// Useful for moving fields instead of copying them.
fn take_field<T>(input: &mut Map<String, Value>, name: &str) -> T
where
    T: DeserializeOwned,
{
    let serde_json::map::Entry::Occupied(entry) = input.entry(name) else {
        panic!("Invalid field removal {:?}", input)
    };
    from_value(entry.remove()).unwrap()
}

impl Node {
    fn new(node_id: &Value, node_ids: &Value) -> Node {
        let node_id = match node_id {
            Value::String(id) => id.clone(),
            _ => panic!("Non-string node_id {}", node_id),
        };
        let node_ids = match node_ids {
            Value::Array(ids) => ids
                .iter()
                .map(|v| match v {
                    Value::String(id) => id.clone(),
                    _ => panic!("Non-string id {}", v),
                })
                .collect(),
            _ => panic!("Non-array node_ids {}", node_ids),
        };
        Node {
            msg_builder: MessageBuilder::new(),
            node_ids,
            node_id,
            neighbors: Vec::new(),
            messages: HashSet::new(),
        }
    }

    fn build_response(
        &mut self,
        request: &Map<String, Value>,
        msg_type: &str,
    ) -> Map<String, Value> {
        let mut response = self.msg_builder.build_message(
            &self.node_id,
            request["src"].as_str().unwrap(),
            msg_type,
        );
        let Value::Object(response_body) = &mut response["body"] else {
            panic!("Invalid response {:?}", response);
        };
        let Value::Object(request_body) = &request["body"] else {
            panic!("Invalid request {:?}", request);
        };
        response_body.insert("in_reply_to".to_owned(), request_body["msg_id"].clone());
        response
    }

    fn handle_init(&mut self, request: Map<String, Value>) {
        let response = self.build_response(&request, "init_ok");
        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }

    fn handle_topology(&mut self, mut request: Map<String, Value>) {
        // Build response before taking fields from `request`.
        let response = self.build_response(&request, "topology_ok");
        let mut body: Map<String, Value> = take_field(&mut request, "body");
        let mut topology: Map<String, Value> = take_field(&mut body, "topology");
        let neighbors: Vec<_> = take_field(&mut topology, &self.node_id);

        self.neighbors = neighbors
            .into_iter()
            .map(|v| match v {
                Value::String(s) => s,
                _ => panic!("Invalid neighbor {:?}", v),
            })
            .collect();
        eprintln!("My neighbors are {:?}", &self.neighbors);

        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }

    fn handle_broadcast(&mut self, mut request: Map<String, Value>) {
        // Build response before taking fields from `request`.
        let response = self.build_response(&request, "broadcast_ok");

        let mut body: Map<String, Value> = take_field(&mut request, "body");
        let msg: u64 = take_field(&mut body, "message");
        let new = self.messages.insert(msg);

        // Broadcast to neighbors.
        for n in self.neighbors.iter() {
            // Interesting note about ownership: If `build_message` was a method on Node this would
            // not compile. `build_message` is mut because we increment `msg_id` and so would
            // mutably borrow the entirety of self, but we already borrowed from self due to
            // iterating over `neighbors`. Therefore we created MessageBuilder so that we could
            // take advantage of split borrowing.
            let mut message = self.msg_builder.build_message(&self.node_id, n, "broadcast");
            message["body"]["message"] = json!(msg);
            let serialized = serde_json::to_string(&message).unwrap();
            println!("{}", serialized);
        }

        let from_neighbor = match &request[&"src".to_owned()] {
            Value::String(src) => self.neighbors.contains(src),
            _ => panic!("Invalid source {:?}", request),
        };
        eprintln!(
            "Received broadcast '{}', which is new? {}. from_neighbor={from_neighbor:?}",
            msg, new
        );
        if from_neighbor {
            // Don't need to ack neighbors.
            return;
        }

        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }

    fn handle_read(&mut self, request: Map<String, Value>) {
        let mut response = self.build_response(&request, "read_ok");
        response["body"]["messages"] = json!(self.messages);
        eprintln!("Received read: {:?}", &response);

        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }
}

fn main() {
    let stdin = std::io::stdin();

    let mut input = String::new();
    let Ok(_) = stdin.read_line(&mut input) else {
        panic!("Failed to read from stdin");
    };
    eprintln!("Received {}", input);
    let Ok(request) = serde_json::from_str::<Map<String, Value>>(&input) else {
        panic!("Failed to parse input: {input}");
    };

    if request["body"]["type"] != "init" {
        panic!("Must initialize node: {}", input);
    }

    eprintln!("Initialized node {}", request["body"]["node_id"]);
    let mut node = Node::new(&request["body"]["node_id"], &request["body"]["node_ids"]);
    node.handle_init(request);

    loop {
        let mut input = String::new();
        let Ok(_) = stdin.read_line(&mut input) else {
            panic!("Failed to read from stdin");
        };
        eprintln!("Received {}", input);
        let Ok(request) = serde_json::from_str::<Map<String, Value>>(&input) else {
            panic!("Failed to parse input: {input}");
        };

        let Value::String(msg_type) = &request["body"]["type"] else {
            panic!("Invalid msg type encoding");
        };

        match msg_type.as_str() {
            "init" => panic!("Already initialized node: {}", input),
            "topology" => node.handle_topology(request),
            "broadcast" => node.handle_broadcast(request),
            "read" => node.handle_read(request),
            _ => panic!("Unknown msg type {}", input),
        }
    }
}
