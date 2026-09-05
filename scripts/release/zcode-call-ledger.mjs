import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const MAX_COUNTED_CALLS = 50;
const OUTCOMES = new Set(['completed', 'failed', 'timed_out', 'cancelled']);

function now() { return new Date().toISOString(); }
function id() { return `call_${crypto.randomUUID()}`; }
function checkText(value, name) {
  if (typeof value !== 'string' || value.length === 0 || value.length > 512 || value.includes('\0')) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
}

/**
 * Durable, fail-closed accounting for real ZCode calls. The JSONL file is
 * replaced atomically after each mutation; a reservation is never deleted.
 */
export class ZCodeCallLedger {
  constructor(file, { maxCalls = MAX_COUNTED_CALLS, lockTimeoutMs = 2000 } = {}) {
    this.file = path.resolve(file);
    this.lockFile = `${this.file}.lock`;
    this.maxCalls = maxCalls;
    this.lockTimeoutMs = lockTimeoutMs;
    fs.mkdirSync(path.dirname(this.file), { recursive: true, mode: 0o700 });
  }

  _read() {
    if (!fs.existsSync(this.file)) return [];
    const lines = fs.readFileSync(this.file, 'utf8').split('\n').filter(Boolean);
    return lines.map((line) => JSON.parse(line));
  }

  _withLock(fn) {
    const started = Date.now();
    let handle;
    while (!handle) {
      try { handle = fs.openSync(this.lockFile, 'wx', 0o600); }
      catch (error) {
        if (error.code !== 'EEXIST' || Date.now() - started >= this.lockTimeoutMs) {
          throw new Error('call ledger is busy; refusing an unaccounted call');
        }
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
      }
    }
    try { return fn(); } finally {
      fs.closeSync(handle);
      fs.unlinkSync(this.lockFile);
    }
  }

  _write(rows) {
    const temporary = `${this.file}.${process.pid}.${crypto.randomUUID()}.tmp`;
    const text = rows.map((row) => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');
    try {
      fs.writeFileSync(temporary, text, { encoding: 'utf8', mode: 0o600, flag: 'wx' });
      fs.renameSync(temporary, this.file);
    } finally {
      try { fs.unlinkSync(temporary); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    }
  }

  reserve({ call_id: callId = id(), scenario_id: scenarioId, reserved_at: reservedAt = now() } = {}) {
    checkText(callId, 'call_id'); checkText(scenarioId, 'scenario_id'); checkText(reservedAt, 'reserved_at');
    return this._withLock(() => {
      const rows = this._read();
      if (rows.some((row) => row.call_id === callId)) throw new Error(`call_id already exists: ${callId}`);
      const counted = rows.filter((row) => row.counted === true).length;
      if (counted >= this.maxCalls) throw new Error(`ZCode call budget exhausted (${this.maxCalls})`);
      const row = { call_id: callId, scenario_id: scenarioId, reserved_at: reservedAt, status: 'reserved', counted: true };
      this._write([...rows, row]);
      return row;
    });
  }

  finalize(callId, outcome, endedAt = now()) {
    checkText(callId, 'call_id');
    if (!OUTCOMES.has(outcome)) throw new TypeError(`invalid call outcome: ${outcome}`);
    checkText(endedAt, 'ended_at');
    return this._withLock(() => {
      const rows = this._read();
      const index = rows.findIndex((row) => row.call_id === callId);
      if (index < 0) throw new Error(`unknown call_id: ${callId}`);
      if (rows[index].status !== 'reserved') throw new Error(`call_id is already finalized: ${callId}`);
      rows[index] = { ...rows[index], ended_at: endedAt, outcome, status: outcome };
      this._write(rows);
      return rows[index];
    });
  }

  abandon(callId, endedAt = now()) {
    checkText(callId, 'call_id'); checkText(endedAt, 'ended_at');
    return this._withLock(() => {
      const rows = this._read();
      const index = rows.findIndex((row) => row.call_id === callId);
      if (index < 0) throw new Error(`unknown call_id: ${callId}`);
      if (rows[index].status !== 'reserved') throw new Error(`call_id is already finalized: ${callId}`);
      rows[index] = { ...rows[index], ended_at: endedAt, status: 'abandoned', counted: true };
      this._write(rows);
      return rows[index];
    });
  }

  rows() { return this._read(); }
  counted() { return this._read().filter((row) => row.counted === true).length; }
}

export function createCallLedger(file, options) { return new ZCodeCallLedger(file, options); }
