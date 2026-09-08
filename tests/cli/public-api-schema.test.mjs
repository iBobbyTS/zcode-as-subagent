import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = path.resolve(import.meta.dirname, '../..');

test('packaged public schema is the reduced zcode_subagent catalog', () => {
  const schema = JSON.parse(fs.readFileSync(path.join(root, 'schema/zcode-subagent-public-api.json'), 'utf8'));
  assert.deepEqual(schema.properties.tools.const, [
    'zcode_subagent_cancel', 'zcode_subagent_close', 'zcode_subagent_list',
    'zcode_subagent_observe', 'zcode_subagent_poll', 'zcode_subagent_respond', 'zcode_subagent_result',
    'zcode_subagent_send', 'zcode_subagent_spawn', 'zcode_subagent_status',
  ]);
  const serialized = JSON.stringify(schema);
  for (const forbidden of ['git', 'worktree', 'artifact', 'budget', 'legacy', 'base_ref', 'HEAD']) {
    assert.equal(serialized.includes(forbidden), false, `public schema exposes ${forbidden}`);
  }
  assert.deepEqual(schema.properties.spawn.additionalProperties, false);
  assert.deepEqual(schema.properties.spawn.properties.write_manifest.items.type, 'string');
  assert.equal(schema.properties.list.properties.limit.default, 100);
  assert.equal(schema.properties.poll.properties.after_revision.default, 0);
  assert.equal(schema.properties.poll.properties.timeout_ms.default, 0);
  assert.equal(schema.properties.result.properties.offset.default, 0);
  assert.equal(schema.properties.result.properties.limit.default, 262144);
  assert.deepEqual(schema.properties.observe.required, ['agent_id']);
  assert.equal(schema.properties.observe.additionalProperties, false);
  assert.deepEqual(schema.properties.contracts.properties.zcode_subagent_observe.input, ['agent_id']);
  assert.deepEqual(schema.properties.contracts.properties.zcode_subagent_observe.output, [
    'schema', 'agent_id', 'service_generation', 'snapshot_seq', 'count_scope', 'tools', 'reasoning', 'coverage',
  ]);
  assert.deepEqual(schema.properties.contracts.properties.zcode_subagent_status.output, [
    'api_surface', 'protocol_version', 'service_generation', 'components', 'capabilities', 'identity',
  ]);
  assert.deepEqual(schema.properties.error_projection.required, ['error']);
  assert.deepEqual(schema.properties.error_projection.properties.error.required, ['code', 'message']);
  assert.deepEqual(Object.keys(schema.properties.error_projection.properties.error.properties).sort(), [
    'agent_id', 'cleanup', 'code', 'component', 'message', 'operation', 'request_id',
  ]);
  assert.deepEqual(schema.properties.contracts.properties.zcode_subagent_result.output, ['task', 'result']);
  assert.deepEqual(schema.properties.contracts.properties.zcode_subagent_spawn.idempotent, false);
  assert.deepEqual(schema.properties.result_projection.required, [
    'outcome', 'final_text', 'partial', 'offset', 'total_bytes', 'next_offset', 'complete',
  ]);
  assert.deepEqual(Object.keys(schema.properties.contracts.properties).sort(), schema.properties.tools.const.slice().sort());
  for (const contract of Object.values(schema.properties.contracts.properties)) {
    assert.ok(Array.isArray(contract.input));
    assert.ok(Array.isArray(contract.output));
  }
});
