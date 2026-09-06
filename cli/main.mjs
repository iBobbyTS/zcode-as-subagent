import fs from 'node:fs';
import { BUSINESS_COMMANDS, PRODUCT_NAME, VERSION, ZCODE_RUNTIME } from './constants.mjs';
import { CliError } from './errors.mjs';
import { runInit, installHooks, installMcp, installPlan, nativeBinary } from './installer.mjs';
import { backupData, cleanupLegacy, purge, restoreData, uninstall } from './maintenance.mjs';
import { platform, productPaths } from './paths.mjs';
import { startService, stopService } from './service.mjs';
import { callDaemon, parseDaemonInput } from './rpc.mjs';

const HELP = `zcode-as-subagent ${VERSION}\n\nUsage: zcode-as-subagent <command> [options]\n\nCommands:\n  help, version               Show basic product information\n  init [--dry-run] [--resume] [--install-hooks] Install and configure the local service\n  hooks install [--dry-run]  Install ZCode policy hooks explicitly\n  install-mcp [codex] [--dry-run|--uninstall] Install or remove the Codex MCP configuration\n  status, diagnose            Inspect local service and runtime state\n  backup --output <dir>       Back up retained product data\n  restore --input <dir>       Verify and restore product data\n  uninstall                   Remove service registration; retain data\n  purge --yes                 Explicitly delete new product data\n  cleanup-legacy --yes        Delete old unpublished installation (no migration)\n`;
const DAEMON_HELP = `  create/spawn, get/poll, list (requires --repository/--workspace scope), send, respond, cancel, result, close\n                             Daemon calls accept --json '<object>' or JSON stdin\n`;

function value(args, name) {
  const index = args.indexOf(name);
  if (index < 0) return undefined;
  const result = args[index + 1];
  if (!result || result.startsWith('--')) throw new CliError('INVALID_ARGUMENT', `${name} requires a value`, 2);
  return result;
}

function output(valueToWrite) {
  process.stdout.write(`${JSON.stringify({ ok: true, product: PRODUCT_NAME, ...valueToWrite }, null, 2)}\n`);
}

export async function main(args) {
  const command = args[0] || 'help';
  if (command === 'help' || command === '--help' || command === '-h') {
    process.stdout.write(HELP + DAEMON_HELP); return;
  }
  if (command === 'version' || command === '--version' || command === '-v') {
    process.stdout.write(`${VERSION}\n`); return;
  }
  if (!BUSINESS_COMMANDS.has(command)) throw new CliError('UNKNOWN_COMMAND', `unknown command: ${command}`, 2);
  if (platform() !== 'darwin') throw new CliError('UNSUPPORTED_PLATFORM', `${command} is supported only on macOS`);

  const paths = productPaths();
  if (command === 'init') {
    output(runInit({ paths, dryRun: args.includes('--dry-run'), resume: args.includes('--resume'), installHooks: args.includes('--install-hooks') })); return;
  }
  if (command === 'hooks') {
    if (args[1] !== 'install') throw new CliError('INVALID_ARGUMENT', 'usage: hooks install [--dry-run]', 2);
    output(installHooks(paths, { dryRun: args.includes('--dry-run') })); return;
  }
  if (command === 'install-mcp') {
    const target = args.slice(1).find((arg) => !arg.startsWith('--')) || 'codex';
    if (target !== 'codex') throw new CliError('INVALID_ARGUMENT', `unsupported MCP target: ${target}`, 2);
    output(installMcp(paths, { dryRun: args.includes('--dry-run'), uninstall: args.includes('--uninstall') })); return;
  }
  if (command === 'status') {
    output({ installed: fs.existsSync(paths.state), launch_agent: fs.existsSync(paths.launchAgent), data: fs.existsSync(paths.data) }); return;
  }
  if (command === 'diagnose') {
    output({ platform: platform(), runtime: ZCODE_RUNTIME, runtime_exists: fs.existsSync(ZCODE_RUNTIME), daemon_binary: nativeBinary('zcode-as-subagentd'), daemon_binary_exists: fs.existsSync(nativeBinary('zcode-as-subagentd')) }); return;
  }
  if (command === 'backup') { output(backupData(value(args, '--output'), paths)); return; }
  if (command === 'restore') { output(restoreData(value(args, '--input'), paths)); return; }
  if (command === 'start') { output(startService(paths)); return; }
  if (command === 'stop') { output(stopService(paths)); return; }
  if (command === 'uninstall') { output(uninstall(paths)); return; }
  if (command === 'purge') {
    if (!args.includes('--yes')) throw new CliError('CONFIRMATION_REQUIRED', 'purge requires --yes');
    output(purge(paths)); return;
  }
  if (command === 'cleanup-legacy') {
    if (!args.includes('--yes')) throw new CliError('CONFIRMATION_REQUIRED', 'cleanup-legacy requires --yes');
    output(cleanupLegacy(paths.home)); return;
  }
  const input = parseDaemonInput(args.slice(1));
  const result = await callDaemon(process.env.ZCODE_AGENTD_SOCKET || paths.socket, command, input);
  output({ command, result });
}

export { HELP, installPlan };
