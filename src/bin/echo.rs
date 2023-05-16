use serde_json::{json, Value};

struct MessageBuilder {
    msg_id: i32,
}

impl MessageBuilder {
    fn new() -> MessageBuilder {
        MessageBuilder { msg_id: 0 }
    }

    fn build_response(&mut self, request: &Value) -> Value {
        let msg_id = self.msg_id;
        self.msg_id += 1;
        json!({
            "src": request["dest"],
            "dest": request["src"],
            "body": {
                "msg_id": msg_id,
                "in_reply_to": request["body"]["msg_id"],
            }
        })
    }

    fn build_init_ok(&mut self, request: &Value) -> Value {
        let mut response = self.build_response(request);
        response["body"]["type"] = "init_ok".into();
        response
    }

    fn build_echo_ok(&mut self, request: &Value) -> Value {
        let mut response = self.build_response(request);
        response["body"]["type"] = "echo_ok".into();
        response["body"]["echo"] = request["body"]["echo"].clone();
        response
    }
}

fn main() {
    let stdin = std::io::stdin();
    let mut msg_builder = MessageBuilder::new();
    loop {
        let mut input = String::new();
        let Ok(_) = stdin.read_line(&mut input) else {
            panic!("Failed to read from stdin");
        };
        eprintln!("Received {}", input);
        let Ok(json) = serde_json::from_str::<Value>(&input) else {
            panic!("Failed to parse input: {input}");
        };

        if json["body"]["type"] == "init" {
            eprintln!("Initialized node {}", json["body"]["node_id"]);
            let init_ok = msg_builder.build_init_ok(&json);
            let serialized = serde_json::to_string(&init_ok).unwrap();
            println!("{}", serialized);
        } else if json["body"]["type"] == "echo" {
            eprintln!("Echoing {}", json["body"]);
            let echo_ok = msg_builder.build_echo_ok(&json);
            let serialized = serde_json::to_string(&echo_ok).unwrap();
            println!("{}", serialized);
        }
    }
}
