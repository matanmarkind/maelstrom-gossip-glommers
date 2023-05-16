use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use std::{assert_eq, eprintln, panic};

use serde_json::{Map, Value};
use tokio::time::sleep;

struct MessageBuilder {
    msg_id: u64,
}

impl MessageBuilder {
    fn new() -> Self {
        Self { msg_id: 0 }
    }
    fn build_message(&mut self, src: &str, dest: &str, msg_type: &str) -> Map<String, Value> {
        let msg_id = self.msg_id;
        self.msg_id += 1;
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
}

struct Node {
    node_id: String,
    neighbors: Vec<String>,
    messages: HashSet<u64>,
    msg_builder: MessageBuilder,
    // {msg_id: message}.
    awaiting_reply: HashMap<u64, String>,
}

// Useful for moving fields instead of copying them.
fn take_field<T>(input: &mut Map<String, Value>, name: &str) -> T
where
    T: serde::de::DeserializeOwned,
{
    let serde_json::map::Entry::Occupied(entry) = input.entry(name) else {
        panic!("Invalid field removal {:?}", input)
    };
    serde_json::from_value(entry.remove()).unwrap()
}

impl Node {
    fn new(node_id: &Value) -> Node {
        let node_id = match node_id {
            Value::String(id) => id.clone(),
            _ => panic!("Non-string node_id {}", node_id),
        };
        Node {
            msg_builder: MessageBuilder::new(),
            node_id,
            neighbors: Vec::new(),
            messages: HashSet::new(),
            awaiting_reply: HashMap::new(),
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

        let src: String = take_field(&mut request, "src");
        let mut body: Map<String, Value> = take_field(&mut request, "body");
        let msg: u64 = take_field(&mut body, "message");
        let new = self.messages.insert(msg);
        eprintln!("Received broadcast '{}', which is new? {}.", msg, new);

        // Ack the broadcast.
        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);

        // Broadcast to neighbors.
        if new {
            for n in self.neighbors.iter().filter(|&n| *n != src) {
                // OWNERSHIP: If `build_message` was a method of Node this would not compile.
                // `build_message` is mut because we increment `msg_id` and so would mutably borrow
                // the entirety of self, but we already borrowed from self due to iterating over
                // `neighbors`. Therefore we created MessageBuilder so that we could take advantage
                // of split borrowing.
                let mut message = self.msg_builder.build_message(&self.node_id, n, "broadcast");
                message["body"]["message"] = serde_json::json!(msg);
                let serialized = serde_json::to_string(&message).unwrap();

                // Add to `awaiting_reply` first so that we don't miss an ack. This shouldn't make a
                // big difference, but may help cut down on unnecessary retries a bit.
                self.awaiting_reply
                    .insert(message["body"]["msg_id"].as_u64().unwrap(), serialized.clone());
                println!("{}", serialized);
            }
        }
    }

    fn handle_broadcast_ok(&mut self, mut request: Map<String, Value>) {
        let mut body: Map<String, Value> = take_field(&mut request, "body");
        let msg_id: u64 = take_field(&mut body, "in_reply_to");
        let present = self.awaiting_reply.remove(&msg_id).is_some();
        eprintln!("Received ack for msg {msg_id} which was already acked? {present}");
    }

    fn handle_read(&mut self, request: Map<String, Value>) {
        let mut response = self.build_response(&request, "read_ok");
        response["body"]["messages"] = serde_json::json!(&self.messages);
        eprintln!("Received read: {:?}", &response);

        let serialized = serde_json::to_string(&response).unwrap();
        println!("{}", serialized);
    }

    // Resend broadcasts messages which are awaiting reply.
    fn retry_messages(&mut self) {
        for message in self.awaiting_reply.values() {
            println!("{}", message);
        }
    }
}

fn await_request(stdin: &std::io::Stdin) -> Map<String, Value> {
    let mut input = String::new();
    let Ok(_) = stdin.read_line(&mut input) else {
        panic!("Failed to read from stdin");
    };
    eprintln!("Received {}", input);
    let Ok(request) = serde_json::from_str::<Map<String, Value>>(&input) else {
        panic!("Failed to parse input: {input}");
    };
    request
}

fn create_node(stdin: &std::io::Stdin) -> Node {
    let request = await_request(stdin);
    assert_eq!(request["body"]["type"], "init", "{request:?}");
    eprintln!("Initialized node {}", request["body"]["node_id"]);
    let mut node = Node::new(&request["body"]["node_id"]);
    node.handle_init(request);
    node
}

// Resends messages that require and haven't received an ack with a set sleep between.
fn spawn_retry_loop(node: Arc<parking_lot::Mutex<Node>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            node.lock().retry_messages();
            sleep(Duration::from_millis(100)).await;
        }
    })
}

#[tokio::main]
async fn main() {
    let stdin = std::io::stdin();

    // We wrap the node in a parking_lot::Mutex to make async simple. There will be lots of blocking
    // between tasks, but that's fine. We just utilize tokio to schedule all of these tasks, we
    // aren't worried about fine grained locking, or ReadWrite locking for performance.
    let node = Arc::new(parking_lot::Mutex::new(create_node(&stdin)));
    spawn_retry_loop(Arc::clone(&node));

    // Main loop.
    loop {
        let request = await_request(&stdin);
        let Value::String(msg_type) = &request["body"]["type"] else {
            panic!("Invalid msg type encoding");
        };

        // Clone node so that we can pass it to the handler and keep a local pointer to the node.
        let node = Arc::clone(&node);
        // Rust creates a new type for each closure, therefore we need to wrap the closures inside
        // of a Box and specify the Trait we care about, since otherwise the compiler sees this
        // as multiple different return types. (polymorphism)
        let handler: Box<dyn FnOnce() + Send> = match msg_type.as_str() {
            "init" => panic!("Already initialized node: {:?}", request),
            "topology" => Box::new(move || node.lock().handle_topology(request)),
            "broadcast" => Box::new(move || node.lock().handle_broadcast(request)),
            "broadcast_ok" => Box::new(move || node.lock().handle_broadcast_ok(request)),
            "read" => Box::new(move || node.lock().handle_read(request)),
            _ => panic!("Unknown msg type {:?}", request),
        };
        // Given that I lock node for the entirety of the async function I'm not sure how
        // valuable it is to run this in a separate task, but it does unblock receiving the next
        // request at least.
        tokio::spawn(async move { handler() });
    }
}
