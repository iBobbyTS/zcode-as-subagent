import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { ZCodeCallLedger } from '../../scripts/release/zcode-call-ledger.mjs';

function ledger(maxCalls = 50) {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'zcode-call-ledger-'));
  return new ZCodeCallLedger(path.join(home, 'calls.jsonl'), { maxCalls });
}

test('reserve/finalize records the required counted wire row', () => {
  const calls = ledger();
  const reserved = calls.reserve({ call_id: 'call-1', scenario_id: 'smoke' });
  assert.deepEqual(Object.keys(reserved).sort(), ['call_id', 'counted', 'reserved_at', 'scenario_id', 'status']);
  const completed = calls.finalize('call-1', 'completed');
  assert.equal(completed.status, 'completed');
  assert.equal(completed.outcome, 'completed');
  assert.equal(calls.counted(), 1);
});

test('call 51 is refused and abandoned reservations remain counted', () => {
  const calls = ledger();
  for (let index = 1; index <= 50; index += 1) {
    calls.reserve({ call_id: `call-${index}`, scenario_id: `scenario-${index}` });
  }
  assert.throws(() => calls.reserve({ call_id: 'call-51', scenario_id: 'scenario-51' }), /budget exhausted/);
  assert.equal(calls.counted(), 50);
  calls.abandon('call-1');
  assert.equal(calls.rows()[0].status, 'abandoned');
});

test('finalization is one-shot and invalid outcomes fail closed', () => {
  const calls = ledger();
  calls.reserve({ call_id: 'call-1', scenario_id: 'smoke' });
  assert.throws(() => calls.finalize('call-1', 'unknown'), /invalid call outcome/);
  calls.finalize('call-1', 'failed');
  assert.throws(() => calls.finalize('call-1', 'completed'), /already finalized/);
});
