# zcode-as-subagent live-agent evidence matrix

The committed matrix exercises the public `zcode_subagent_*` catalog and
`build|edit|plan|yolo` permission contract. It is intentionally offline and
never invokes a real model. A missing native daemon/runtime payload is an
`EVIDENCE_GAP`, not a passing runtime claim.

The release gate also checks these explicit negative guarantees: no migration
or old aliases; no automatic download/upgrade; no provider or credential
management; no remote daemon, multi-tenant service, Windows daemon, GUI,
Rosetta, Git/worktree/base_ref/access_mode integration, or second supervisor.

This directory separates committed small-scale tests from local Git-based
Agent fixtures and disposable execution state.

- `non-git-based/` is committed. It contains the small fake-runtime, transport,
  facade and harness tests that do not require a fixture Git repository.
- `git-based/` is local and ignored. It contains complex Agent scenario source
  templates whose workspaces include independent Git repositories.
- `workspace/` is local and ignored. Every test execution must copy its source
scenario here before reset, verification, or result collection.

Source scenarios are immutable inputs. Test code must use
`non-git-based/fixture_workspace.py` to create a unique execution directory and
materialize a scenario. Results, transcripts, logs, stores, temporary Git
repositories, and imported historical evidence stay under `workspace/`.

`non-git-based/real_completion_case.py` is an explicit real-runtime evidence
flow. Run it only when the local daemon and official ZCode runtime are ready:

```sh
python3 tests/live-agent/non-git-based/real_completion_case.py
```

It performs one isolated read-only `plan` lifecycle (`spawn`, repeated `poll`,
`result`, `close`) and emits the observed task/activity/result payloads. The
harness does not classify the outcome or decide goal success; that decision is
left to the executing Agent or human. It exits non-zero only when a transport or
protocol call fails.

The runner records observable runtime facts and safety invariants; it does not
replace the task executor's or human evaluator's judgment of whether a goal was
achieved. A task that achieves its stated goal may be classified as success or
success-with-gap when bounded evidence is incomplete. `FAILED` is reserved for
an outcome that did not achieve the goal. Artifact hashes and repository
identity remain integrity evidence, not a substitute for goal judgment.
