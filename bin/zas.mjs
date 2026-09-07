#!/usr/bin/env node
import { main } from '../cli/main.mjs';

main(process.argv.slice(2)).catch((error) => {
  const code = typeof error?.code === 'string' ? error.code : 'INTERNAL_ERROR';
  process.stderr.write(`${JSON.stringify({ ok: false, error: { code, message: error.message, ...(error.agentId ? { agent_id: error.agentId } : {}) } })}\n`);
  process.exitCode = Number.isInteger(error?.exitCode) ? error.exitCode : 1;
});
