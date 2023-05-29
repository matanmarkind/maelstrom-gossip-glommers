use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::panic;

use itertools::Itertools;
use serde_json::{json, Map, Value};

struct Node {
    inner: maelstrom_gossip_glommers::Node,
    data: HashMap<i64, Vec<i64>>,
}

impl Node {
    fn new(inner: maelstrom_gossip_glommers::Node) -> Self {
        Self { inner, data: HashMap::new() }
    }

    fn handle_txn(&mut self, mut request: Map<String, Value>) {
        // Build response before taking fields from `request`.
        let mut response = self.inner.build_response(&request, "txn_ok");
        let Value::Object(response_body) = &mut response["body"] else {
            panic!("Invalid response {:?}", response);
        };
        let mut response_txn = Vec::new();

        let mut request_body: Map<String, Value> =
            maelstrom_gossip_glommers::take_field(&mut request, "body");
        let request_txn: Vec<Value> =
            maelstrom_gossip_glommers::take_field(&mut request_body, "txn");

        for txn in request_txn {
            let Value::Array(txn) = txn else {
                panic!("Invalid transaction {:?}", txn);
            };
            let Some((func, key, val)) = txn.into_iter().collect_tuple() else {
                panic!("Transaction cannot be decomposed.");
            };
            let Value::String(func) = func else {
                panic!("Invalid function {:?}", func);
            };
            let Value::Number(key) = key else {
                panic!("Invalid key {:?}", key);
            };
            let key = key.as_i64().unwrap();

            match func.as_str() {
                "r" => self.read(key, &mut response_txn),
                "append" => self.append(key, val, &mut response_txn),
                _ => panic!("Unknown txn function {:?}", func),
            }
        }

        response_body.insert("txn".to_string(), json!(response_txn));
        let serialized = serde_json::to_string(&response).unwrap();
        eprintln!("{}", &serialized);
        println!("{}", serialized);
    }

    fn read(&self, key: i64, txn: &mut Vec<Value>) {
        let ret_val = match self.data.get(&key) {
            None => Value::Null,
            Some(v) => Value::Array(v.iter().map(|x| Value::from(*x)).collect()),
        };
        txn.push(json!(["r", key, ret_val]));
    }

    fn append(&mut self, key: i64, val: Value, txn: &mut Vec<Value>) {
        txn.push(json!(["append", key, val]));
        let Value::Number(val) = val else {
            panic!("Invalid append value: {:?}", val)
        };
        let val = val.as_i64().unwrap();

        match self.data.entry(key) {
            Entry::Occupied(mut entry) => entry.get_mut().push(val),
            Entry::Vacant(entry) => {
                entry.insert(vec![val]);
            }
        };
    }
}

// Strict serializability means we aren't spawning any tasks. Once every stage is complete will go
// back and restructure to take advantage of async environ.
#[tokio::main]
async fn main() {
    let stdin = async_std::io::stdin();
    let mut node = Node::new(maelstrom_gossip_glommers::create_node(&stdin).await);

    // Main loop.
    loop {
        let request = maelstrom_gossip_glommers::await_request(&stdin).await;
        let Value::String(msg_type) = &request["body"]["type"] else {
            panic!("Invalid msg type encoding");
        };

        match msg_type.as_str() {
            "init" => panic!("Already initialized node: {:?}", request),
            "txn" => node.handle_txn(request),
            _ => panic!("Unknown msg type {:?}", request),
        };
    }
}
