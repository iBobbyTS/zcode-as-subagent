use serde_json::{json, Value};
use std::{
    io::Write,
    process::{Command, Stdio},
};

fn discover() -> Vec<Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_zcode-as-subagent-mcp"))
        .env(
            "ZCODE_AGENTD_SOCKET",
            "/tmp/zcode-subagent-mcp-test-unused.sock",
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let frames = [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"zcode_subagent_result","arguments":{"agent_id":"missing-agent"}}}),
        json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"zcode_subagent_result","arguments":{}}}),
        json!({"jsonrpc":"2.0","id":5,"method":"unknown/protocol-method","params":{}}),
        json!({"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"zcode_subagent_spawn","arguments":{"repository":"/tmp/repository","prompt":"test"}}}),
    ];
    {
        let stdin = child.stdin.as_mut().unwrap();
        for frame in frames {
            writeln!(stdin, "{frame}").unwrap();
        }
    }
    drop(child.stdin.take());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[test]
fn stdio_catalog_is_exactly_the_generic_ten_tools() {
    let frames = discover();
    let tools = frames.iter().find(|frame| frame["id"] == 2).unwrap()["result"]["tools"]
        .as_array()
        .unwrap();
    let names = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, zcode_subagent_mcp::PUBLIC_TOOLS);
    let schema = serde_json::to_string(tools).unwrap();
    for forbidden in [
        concat!("zcode_", "review_"),
        concat!("zcode_subagent_", "system_ensure_ready"),
        concat!("zcode_subagent_", "get"),
        concat!("zcode_subagent_", "events"),
        concat!("zcode_subagent_", "wait"),
        concat!("review", "_id"),
        concat!("report_", "markdown"),
        concat!("check_", "report"),
        concat!("artifact", "_intents"),
        "interrupt_and_continue",
        "semantic_soft_timeout_ms",
        "semantic_hard_timeout_ms",
    ] {
        assert!(!schema.contains(forbidden), "catalog leaked {forbidden}");
    }
    let by_name = |name: &str| tools.iter().find(|tool| tool["name"] == name).unwrap();
    assert_eq!(
        by_name("zcode_subagent_spawn")["annotations"]["idempotentHint"],
        false
    );
    assert_eq!(
        by_name("zcode_subagent_send")["annotations"]["idempotentHint"],
        false
    );
    assert_eq!(
        by_name("zcode_subagent_list")["inputSchema"]["properties"]["limit"]["default"],
        100
    );
    assert_eq!(
        by_name("zcode_subagent_poll")["inputSchema"]["properties"]["after_revision"]["default"],
        0
    );
    assert_eq!(
        by_name("zcode_subagent_result")["inputSchema"]["properties"]["limit"]["default"],
        80 * 1024
    );
}

#[test]
fn stdio_business_failure_is_structured_and_protocol_failure_is_json_rpc_error() {
    let frames = discover();
    let business = frames.iter().find(|frame| frame["id"] == 3).unwrap();
    assert!(business.get("error").is_none(), "{business}");
    let result = &business["result"];
    assert_eq!(result["isError"], true);
    assert_eq!(
        result["content"][0]["text"],
        "daemon_unavailable: subagent daemon is unavailable"
    );
    assert_eq!(
        result["structuredContent"]["error"]["code"],
        "daemon_unavailable"
    );
    assert_eq!(
        result["structuredContent"]["error"]["component"],
        "daemon_transport"
    );
    assert_eq!(result["structuredContent"]["error"]["operation"], "result");
    assert_eq!(
        result["structuredContent"]["error"]["agent_id"],
        "missing-agent"
    );
    assert!(result["structuredContent"]["error"]["request_id"]
        .as_str()
        .is_some_and(|value| value.starts_with("subagent-mcp-")));

    // rmcp rejects tool argument decoding before the product handler. Keep
    // that SDK-owned result distinct from our typed execution error.
    let invalid_arguments = frames.iter().find(|frame| frame["id"] == 4).unwrap();
    assert_eq!(invalid_arguments["result"]["isError"], true);
    assert!(invalid_arguments["result"]
        .get("structuredContent")
        .is_none());

    let protocol = frames.iter().find(|frame| frame["id"] == 5).unwrap();
    assert!(protocol.get("result").is_none(), "{protocol}");
    assert!(protocol["error"]["code"].is_number(), "{protocol}");

    let spawn = frames.iter().find(|frame| frame["id"] == 6).unwrap();
    assert_eq!(spawn["result"]["isError"], true);
    assert_eq!(
        spawn["result"]["structuredContent"]["error"]["code"],
        "daemon_unavailable"
    );
    assert_eq!(
        spawn["result"]["structuredContent"]["error"]["operation"],
        "spawn"
    );
    assert!(spawn["result"]["structuredContent"]["error"]
        .get("agent_id")
        .is_none());
}
