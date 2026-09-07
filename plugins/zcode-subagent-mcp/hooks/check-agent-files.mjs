#!/usr/bin/env node
import { createAgentFileHookOutput, evaluateAgentFileInput } from '../lib/agent-file-policy.mjs';

let raw = '';
process.stdin.setEncoding('utf8');
for await (const chunk of process.stdin) raw += chunk;
try {
  const input = JSON.parse(raw);
  // Hooks are optional and must not claim ordinary, unmanaged ZCode sessions.
  if (process.env.ZCODE_AGENT_POLICY !== '1') process.exit(0);
  process.stdout.write(`${JSON.stringify(createAgentFileHookOutput(evaluateAgentFileInput(input)))}\n`);
} catch {
  if (process.env.ZCODE_AGENT_POLICY !== '1') process.exit(0);
  // Never echo input, paths, or parser details: hook failures are denied and redacted.
  process.stdout.write(`${JSON.stringify(createAgentFileHookOutput({
    decision: 'deny',
    reason: 'zcode-agent-file-policy/v1.0.0: hook_internal_error',
  }))}\n`);
}
