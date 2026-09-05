import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const root = path.resolve(import.meta.dirname, '../..');

test('packaged public schema is the reduced zcode_subagent catalog', () => {
  const schema = JSON.parse(fs.readFileSync(path.join(root, 'schema/zcode-subagent-public-api.json'), 'utf8'));
  assert.deepEqual(schema.properties.tools.const, [
    'zcode_subagent_cancel', 'zcode_subagent_close', 'zcode_subagent_list',
    'zcode_subagent_poll', 'zcode_subagent_respond', 'zcode_subagent_result',
    'zcode_subagent_send', 'zcode_subagent_spawn', 'zcode_subagent_status',
  ]);
  const serialized = JSON.stringify(schema);
  for (const forbidden of ['git', 'worktree', 'artifact', 'budget', 'legacy', 'base_ref', 'HEAD']) {
    assert.equal(serialized.includes(forbidden), false, `public schema exposes ${forbidden}`);
  }
  assert.deepEqual(schema.properties.spawn.additionalProperties, false);
  assert.deepEqual(schema.properties.spawn.properties.write_manifest.items.type, 'string');
});
