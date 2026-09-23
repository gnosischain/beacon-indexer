# AGENTS.md — beacon-indexer

Fork-aware ELT indexer for a beacon (consensus-layer) node: beacon REST API → `load` verbs write raw JSON
to `consensus.raw_*` → `transform` derives the structured `consensus.*` tables. Progress lives in
`consensus.load_state_chunks` and `consensus.transformer_progress`, never in the pod. The rule that
matters most: **exactly one writer per stage, nothing in the app enforces it, and every state table is
read with `FINAL`** (migrations/004_state_tables.sql:11,26).

## Where this runs (production)

GKE Autopilot, deployed by Terraform from the private deployments repository
(`gnosisdevops/infrastructure-gnosis-analytics`, the beacon-indexer stack in its GKE tree).
Image `ghcr.io/gnosischain/gc-beacon-indexer`: every push to `main` publishes `:latest` and `:<short-sha>`
(multi-arch, .github/workflows/build-and-release.yml:3-57), but production pins an explicit tag and digest
in the stack, so a merge is not a deploy: bump the pin, and apply any new migration first.

Every workload sets the container `command` (deployments.tf:138-141, cronjob.tf:96), which in Kubernetes
**replaces** the image `ENTRYPOINT` (Dockerfile:27): docker-entrypoint.sh (ClickHouse wait loop,
`PYTHONPATH=/app`) never runs in production, and an unreachable ClickHouse crashes the process after the
connect timeout (src/services/clickhouse.py:40-48) → CrashLoopBackOff. The entrypoint runs only under
docker compose / `docker run`, or in a Job that sets `args` instead of `command`; it waits for `SELECT 1`
only when `STORAGE_BACKEND` is exactly `clickhouse` (case-sensitive; unset or empty counts, docker-entrypoint.sh:8-10).
Any other value skips the wait and creates the Parquet dir (docker-entrypoint.sh:28-32), yet the app still uses
ClickHouse unless the value is `parquet` in any case (src/services/storage_factory.py:7-14). A Job that sets
neither runs the entrypoint with no args and `exec`s nothing: exit 0, `Completed`, no work (docker-entrypoint.sh:35).

| Role | Runs (exact command) | Schedule | `ENABLED_LOADERS` | Writes (`consensus.`) |
|---|---|---|---|---|
| Continuous raw ingest Deployment | `python -m src.main load realtime` | continuous, 1 replica, `Recreate` | `blocks,rewards,data_column_sidecars` | `raw_blocks`, `raw_rewards`, `raw_data_column_sidecars`, `load_state_chunks`; every start `raw_specs`, `specs`, `time_helpers` (+ `genesis`/`raw_genesis` if empty) |
| Continuous transform Deployment | `python -m src.main transform run --continuous` | continuous, 1 replica, `Recreate`, **no beacon credentials** | `blocks,validators,rewards,data_column_sidecars` | tables mapped in src/services/maintenance.py:26-63, `transformer_progress` |
| Daily validators CronJob | `python -m src.main load backfill` (`START_SLOT=0`, `END_SLOT` unset → head) | `0 2 * * *` UTC, `Forbid`, starting deadline 3600 s, backoff 2; ~4 h | `validators` | `raw_validators` (last slot of each UTC day), `load_state_chunks`, `raw_specs`/`specs`/`time_helpers` |

Production env (locals.tf:40-76): `CLICKHOUSE_DATABASE=consensus`, `CLICKHOUSE_PORT=443`,
`CLICKHOUSE_SECURE=true`, `CHUNK_SIZE=100`, `VALIDATOR_MODE=daily`, `REALTIME_SLOT_DELAY=700` (realtime),
`TRANSFORM_CHUNKS_PER_BATCH=1` (transform), `BACKFILL_WORKERS=1` (cron), `METRICS_ENABLED=true` (gates the
whole `/metrics` + `/health` server, src/config.py:68, src/cli.py:104-108), `METRICS_PORT=9090`. Beacon
URL/key reach only realtime and the cron; the key is the query parameter `${BEACON_API_KEY_PARAM:-apiKey}`
(src/config.py:14-15, src/services/beacon_api.py:70-72). `ELECTRA_START_SLOT`/`FULU_START_SLOT` are unset:
code defaults apply (src/config.py:49,57).

Code defaults differ from production (src/config.py:34,42,46,63): `ENABLED_LOADERS=blocks,validators,specs,genesis`,
`BACKFILL_WORKERS=4`, `REALTIME_SLOT_DELAY=0`, `TRANSFORM_CHUNKS_PER_BATCH=10`. A one-shot Job cloned from the daily
CronJob inherits only its env (locals.tf:64-76, cronjob.tf:128-161: `ENABLED_LOADERS=validators`, no
`TRANSFORM_CHUNKS_PER_BATCH`). Set `TRANSFORM_CHUNKS_PER_BATCH=1` for any transform verb and name every needed loader
in `ENABLED_LOADERS` explicitly.

**Local-only:** `Makefile`, `docker-compose.yml`, `.env`/`.env.example`, the `parquet` backend. Compose runs
the locally built `beacon-indexer:latest` — your working tree at the last `make build`
(docker-compose.yml:4,26,56,85,108,131) — not the production image. src/config.py:5 runs `load_dotenv()` at
import, so every bare `python -m src.main …` and `python -m pytest` from the repo root, and any
`PYTHONPATH=. python scripts/*.py` (scripts need the repo root on the path, scripts/migrate.py:9-11),
silently use whatever `CLICKHOUSE_HOST` and beacon credentials the gitignored `.env` names (.gitignore:8).
Compose forwards only the variables listed per service (docker-compose.yml:6-148; the image has no `.env`,
Dockerfile:15-18): `TRANSFORM_*`, `BEACON_API_KEY_PARAM`, `ELECTRA_START_SLOT`, `FULU_START_SLOT` and `METRICS_*`
never reach a compose container, so `make transform`, `make transform-continuous`, `make maintenance-transform`
and any `transform` via the `maintenance` service run `TRANSFORM_CHUNKS_PER_BATCH=10` (src/config.py:63).
`make status` is the only target that passes the whole `.env` (`--env-file`, Makefile:37).
Production runs only the three commands above, with the entrypoint bypassed. **Single writer** is enforced
only by Kubernetes shape: `Recreate` + `replicas = 1` (deployments.tf:57-66, locals.tf:24),
`concurrencyPolicy: Forbid` (cronjob.tf:22).

**Deliberately not deployed:** the migration, historical-backfill and one-shot transform Jobs
(deployments.tf:4-7); probes (deployments.tf:9-14); the Electra `pending_*` loaders; `VALIDATOR_MODE=all_slots`.
**Migrations never run on start:** nothing under `src/` reads `migrations/` and no workload runs
`scripts/migrate.py`. A new `migrations/NNN_*.sql` reaches production only when a human runs a one-shot Job
cloned from the daily CronJob with `command = ["python", "-m", "scripts.migrate"]` (or env `PYTHONPATH=/app`,
or `args` instead of `command` so the entrypoint sets it) → one-shot-jobs runbook, before the image that
needs it is pinned; otherwise the first `transform` touching the new column fails. A bare
`python scripts/migrate.py` as `command` fails with `ModuleNotFoundError`: it puts only `/app/src` on
`sys.path` (scripts/migrate.py:9-11) and src/services/clickhouse.py:8 imports `src.config`.

## Modes and commands — the complete list

Verbs are `python -m src.main …` (src/main.py:7); `python -m src.cli …` is equivalent (src/cli.py:365-366) and
the same rules apply to it. "Live writer" = the three production workloads. Row detail lives in Hazards.

| Invocation | What it does | Writes? | Safe beside the live writer? | Required args / env | Source |
|---|---|---|---|---|---|
| `load realtime` | Loads complete `CHUNK_SIZE`-aligned chunks ending ≤ head − `REALTIME_SLOT_DELAY`. Per loader: `completed` if any slot loaded; `failed` if none did, if any slot hit a beacon error (a 404 counts for the state loaders `validators`/`pending_*`, and is an empty slot for `blocks`/`rewards`/`data_column_sidecars`), or if the raw insert raised | yes | **No** — it is the writer | beacon env, CH env, `ENABLED_LOADERS` | src/cli.py:25,144-145; src/services/loader.py:44-118,137-241,349-374; src/loaders/base.py:74-105; src/services/beacon_api.py:86-97 |
| `load backfill [--start-slot N] [--end-slot M]` | Queues `pending` chunks for `[N, M)`; `BACKFILL_WORKERS` processes drain **every** `pending` chunk of the enabled loaders; exits after 3 idle rounds, 0 whatever failed | yes | Only with loaders disjoint from realtime's, or strictly behind its frontier | beacon env, CH env, `ENABLED_LOADERS` | src/cli.py:28-32,147-166; src/services/loader.py:463-724,1062-1209; src/services/clickhouse.py:137-151 |
| `transform run [--continuous] [--batch-size N]` | Parses `completed` load chunks lacking a `completed` progress row; `--continuous` polls every 10 s (30 s after an error), else = `transform batch`; `--batch-size` ignored | yes | **No** — a second transformer selects the same chunks | CH env, `ENABLED_LOADERS`, `TRANSFORM_*` | src/cli.py:39-41,178-181; src/services/transformer.py:131-142,150-268 |
| `transform batch [--batch-size N]` | As `run`, exits after 3 empty rounds, or with 0 after an exception | yes | **No** | as above | src/cli.py:44-45,183-185; src/services/transformer.py:153,225-233 |
| `transform reprocess [--start-slot N] [--end-slot M] [--batch-size N]` | **Stub**: logs "Reprocess not implemented yet", exit 0 | no | n/a | `--end-slot` unless `END_SLOT` env is set | src/cli.py:47-53,187-204; src/config.py:41; src/services/transformer.py:610-618 |
| `maintain check --start-slot N --end-slot M [--detailed]` | Lists `failed` and completed-but-untransformed chunks (latter `LIMIT 100`); blind to missing chunk rows | stale-`processing` sweep | yes | both slot args; CH env | src/cli.py:70-74,209-271; src/services/maintenance.py:65-75,577-667 |
| `maintain fix --start-slot N --end-slot M [--loaders a,b] [--force] [--dry-run]` | `failed` (`--force`: also `completed`) chunks: deletes raw **and** structured rows, reloads, retransforms. `--dry-run` prints the selection, but like every `maintain` sub-verb except `refresh-specs` it first runs the stale-`processing` sweep, which writes `transformer_progress` | yes, destructive (`--dry-run`: sweep only) | **No** near the frontier; behind it only with beacon creds and an `ENABLED_LOADERS` superset | both slot args; beacon env; CH env | src/cli.py:60-67,220-251; src/services/maintenance.py:65-138,207-374,495-575 |
| `maintain reset --start-slot N --end-slot M [--loaders a,b] [--status failed\|claimed\|completed]` | Inserts `pending` rows for chunks in `--status` (default `failed`); no `--loaders` = every loader | yes | yes for `failed`/`claimed` | both slot args; CH env | src/cli.py:77-82,273-289; src/services/maintenance.py:669-715 |
| `maintain refresh-specs` | Genesis if absent; appends `raw_specs`, upserts `specs`/`time_helpers` | yes | yes (every start does the same) | beacon env, CH env | src/cli.py:85,216-218,299-319; src/loaders/genesis.py:24-101; src/loaders/specs.py:24-91 |
| `fork info --slot N` / `--epoch N` / `fork list` | Prints fork config from `specs`/`genesis` + config/forks.yaml | no | yes | CH env | src/cli.py:88-97,321-363; src/services/fork.py:72-74,147-165,405-445 |
| no verb | Usage, exit 0 | no | yes | — | src/cli.py:115-117 |
| `load` / `transform` / `fork` without sub-verb; `fork info` with neither `--slot` nor `--epoch` | Connects to ClickHouse first (`load` also opens a beacon session), then prints usage, exit 0; an unreachable ClickHouse exits non-zero | no | yes | CH env | src/cli.py:133-142,169,176,207,324,341-342,363; src/services/clickhouse.py:43 |
| `maintain` without sub-verb | Runs the stale-`processing` sweep, then usage | `transformer_progress` | yes | CH env | src/cli.py:209-223,291-292; src/services/maintenance.py:72-73 |
| `python -m scripts.migrate` (compose: `python scripts/migrate.py`) | Runs every `migrations/*.sql` in filename order; no ledger; all `IF NOT EXISTS` | DDL | Only as a human-approved one-shot Job for a new migration, before the image that needs it is pinned; never from a laptop | CH env; repo root or `/app` on `sys.path` | scripts/migrate.py:9-11,14-66 |
| `scripts/chunks.py overview\|status\|test\|debug` | Chunk counts with `FINAL`; `debug` suggests `OPTIMIZE … FINAL` — never run it | no | yes | CH env | scripts/chunks.py:15-163 |
| `scripts/maintenance.py failed\|gaps\|status\|summary\|recommendations` | Reports; `gaps` counts distinct slots, so empty slots read as missing | no | yes | CH env, `ENABLED_LOADERS` | scripts/maintenance.py:87-150,416-442 |
| `scripts/transformer_status.py` | `transformer_progress` summary, no sweep | no | yes | CH env | scripts/transformer_status.py:16-80 |
| `make help\|build\|status\|logs\|clean` (local) | Prints `## ` comments; builds local `beacon-indexer:latest`; read-only `docker run --env-file .env … scripts/chunks.py overview`; tails compose; `docker compose down -v` + `docker system prune -f` | no | yes | `.env` | Makefile:10-17,36-44 |
| `make migration\|backfill\|realtime\|transform\|transform-continuous` (local) | Compose profiles: `scripts/migrate.py`, `load backfill`, `load realtime`, `transform batch`, `transform run --continuous` | yes | never with a production `CLICKHOUSE_HOST` | `.env` | Makefile:20-33; docker-compose.yml:3-127 |
| `docker compose run --rm maintenance <verb>` (local) | No fixed `command`, beacon creds + loader env: runs any verb, writers included | via the verb | never with a production host | `.env` | docker-compose.yml:130-152 |
| `make maintenance-*` (16 targets, local) | Wraps `maintain check\|fix\|reset` and `scripts/maintenance.py`; `-fix*`/`-reset*` destructive; `maintenance-transform` = `transform batch` | via the verb | as the wrapped verb | `MAINTENANCE_*`, `LOADERS` | Makefile:4-8,46-145 |

Loaders (src/loaders/__init__.py:11-33): `blocks`, `validators`, `specs`, `genesis`, `rewards`,
`data_column_sidecars`, `pending_consolidations`, `pending_deposits`, `pending_partial_withdrawals`.
`genesis`/`specs` load on every realtime/backfill start (src/services/loader.py:52,416,1008-1060). An
unregistered name is dropped from loader instances (src/loaders/__init__.py:29-31), yet `load backfill` inserts `pending` chunks for
it (src/services/loader.py:466-468,651-687) that no worker claims (:1086-1091). `validators` and `pending_*`
read beacon **state** (archive node): a 404 fails the chunk (src/services/beacon_api.py:86-97;
src/loaders/validators.py:260-274 in daily mode, :215-229 in `all_slots`). `blocks`/`rewards`/
`data_column_sidecars` treat 404 as an empty slot (src/services/beacon_api.py:49,86-89);
`data_column_sidecars` is empty below `FULU_START_SLOT` (src/loaders/data_column_sidecars.py:16-20). Unlike
`validators`, `pending_*` have no daily gate: one archive state call per slot, 100 per chunk
(src/loaders/pending_consolidations.py:28-33, src/loaders/base.py:74-105), despite the "daily" comments in
migrations/012_pending_queues.sql:11-13; below a non-zero `ELECTRA_START_SLOT` they return nothing. Neither
runbook 21 nor the stack records whether the production beacon URL serves historical state (URL and key come
from a secret, locals.tf:86-89): confirm archive access with the maintainer before any historical
`validators`/`pending_*` repair, since a head-only endpoint 404s every such chunk.

## What does not exist, or does not do what its name says

- `src/services/clickhouse.py` defines `insert_batch`, `_insert_{small,medium,large}_batch`, `_do_insert` and `insert_batch_optimized` twice. The later definitions (:430, :571, :594, :618, :652, :795) shadow the earlier ones (:111-386), so read and cite the later ones.
- `transform reprocess` is a stub (src/services/transformer.py:610-618); `--batch-size` is never used (:131-142) — throughput is `TRANSFORM_CHUNKS_PER_FETCH`/`_PER_BATCH` (src/config.py:63-65).
- The Parquet transformer is `pass` (src/services/transformer.py:144-148), unlike docs/PARQUET_GUIDE.md:108,121-126 and docs/STORAGE_BACKEND.md:46-57.
- `make maintenance-check|maintenance-quick-check|maintenance-fix-recent` (README.md:166,181,184,406) and `make dev-backfill|dev-transform` (docs/DEVELOPMENT.md:25-26) have no rule (Makefile:10-115); the `committees` loader (docs/DEVELOPMENT.md:245,572) is a tutorial example.
- The Makefile "recent range" defaults to slots 9,000,000–9,100,000 (Makefile:7-8), not recent on Gnosis.
- `/health` is set `ok` once at startup, never updated (src/cli.py:108, src/observability.py:146-155,170-177).
- Doc defaults disagree with code: `ENABLED_LOADERS` and `CHUNK_SIZE` (src/config.py:34,43 vs docs/CONFIGURATION.md:42,52 and .env.example:30,49), and the public docs page's "ClickHouse Settings" port/database/secure defaults `8443`/`consensus`/`true` (code: `9000`/`beacon_chain`/`false`, src/config.py:22,25,26). Production sets port, database and secure explicitly (locals.tf:41-43,54-56,65-67). README.md:96-102 links wrong doc file names.
- **Exit 0 does not mean it ran.** `load backfill` exits 0 even if every chunk failed (src/services/loader.py:1148-1158,1204-1209): a succeeded daily CronJob run proves nothing — check `load_state_chunks FINAL` for `failed` validators rows. It also exits 0 without writing when `start >= end` or the head fetch 404s (src/cli.py:153-164, src/services/beacon_api.py:214-219). Likewise `transform reprocess` without `--end-slot` or `END_SLOT`, `maintain` under any `STORAGE_BACKEND` but `clickhouse`, missing sub-verbs (src/cli.py:169,191-198,207,212-214,292,363), `transform batch` after an exception (src/services/transformer.py:225-233). `maintain fix` exits 0 without retransforming when no 20-chunk reload batch fully succeeded (every reload failing included), leaving the range deleted (src/services/maintenance.py:122-134,359); once one batch succeeds it retransforms every selected chunk, and those whose reload failed get a `completed` 'No data' `transformer_progress` row (src/services/transformer.py:289-291). `maintain reset` prints 'Reset 0 chunks' and exits 0 on any query error (src/services/maintenance.py:713-715). After `fix`, verify raw-table rows over the range.
- The daily CronJob is a recurring 0→head catch-up skipping `completed` validators target slots (src/services/loader.py:500-507,621-635), not a historical replay.
- Every `maintain` sub-verb except `refresh-specs` (`check`, `fix` including `--dry-run`, `reset`, and bare `maintain`) runs the stale-`processing` sweep first (src/cli.py:216-223 → src/services/maintenance.py:72-73 → src/services/transformer.py:103-126), which writes `failed` rows into `transformer_progress`.
- **A green chunk can be empty.** A 404 on a block-style endpoint counts as an empty slot (src/loaders/base.py:82-86, src/services/beacon_api.py:86-89), and backfill marks a chunk `completed` regardless of rows (src/services/loader.py:1133-1136): a node serving head/specs but with pruned old blocks yields green, empty chunks on a historical blocks/rewards backfill. A wholly wrong URL path instead fails realtime at startup (specs 404, src/services/loader.py:1030-1038).

## Hazards

- **Realtime skips an outage window silently.** Resume point = `max(slot)` over all four raw tables (incl. `raw_validators`, written near head by the cron) rounded up to the next chunk (src/services/loader.py:53,67,349-374). After any outage run the runbook "Gap" check over `[frontier_before, head]` and `load backfill` the holes.
- **Realtime does not come back for a loader's failed chunk** once a sibling loader succeeded on it or a later chunk succeeded (src/services/loader.py:97-104,229-241); only a chunk every loader failed, with nothing after it succeeding, is retried. Repair failed realtime chunks either with a scoped `load backfill --start-slot N --end-slot M` whose `ENABLED_LOADERS` names only the failed loader (non-destructive: it re-queues non-`completed` chunks, src/services/loader.py:500-507,547-580; keep it strictly behind the frontier), or with `maintain fix` (destructive: delete, then reload).
- **An unclean kill (OOMKilled, eviction, pod deletion) leaves chunks `claimed`.** The cron's `finally` reset to `pending` never runs on SIGKILL (src/services/loader.py:1176-1198); realtime resumes from max(raw slot) (:53,349-374), so it re-processes the in-flight chunk only when no loader had stored rows for it yet (src/loaders/base.py:102-103). No worker claims `claimed` chunks (src/services/clickhouse.py:147) and `fix` skips them (src/services/maintenance.py:507-510), but any later `load backfill` whose range covers them re-queues them as `pending`, because chunk generation skips only `completed` chunks (src/services/loader.py:500-507,571-580,636-645). Claimed `validators` chunks are re-queued by the next daily cron run; claimed `blocks`/`rewards`/`data_column_sidecars` chunks need a scoped `load backfill` with those loaders. So the runbook "Safe stop and restart" step `maintain reset --status claimed` is not required first.
- **`maintain reset` is half a repair.** No realtime or transform workload claims `pending`. `load backfill` workers claim every `pending` chunk of their loaders, whatever the range (src/services/clickhouse.py:137-151): `validators` drain at the next 02:00 cron run; blocks/rewards/data_column_sidecars need a manual `load backfill`. Without `--loaders` every loader is reset (src/services/maintenance.py:681-696).
- **A scoped `load backfill` is scoped only for chunk creation**: its workers also drain `pending` chunks outside `[N, M)` for the same loaders.
- **`load backfill` ranges.** `M` exclusive (src/services/loader.py:554), default head. Slot-based loaders floor `N` to a `CHUNK_SIZE` boundary (:547); `validators`/`daily` gets one single-slot chunk per daily target (:621-646). Chunks not `completed` are re-queued (:500-507), and completed ones too for all `pending_*` loaders (no completed-cache, :495) and for the floored first chunk when `N` is off-boundary (:505 vs :547).
- **Status writes are swallowed; raw inserts are not.** The effective `insert_batch` (src/services/clickhouse.py:430-569, see duplicates above) raises on any insert error, so a failed raw insert fails the chunk (src/loaders/base.py:102-103 → src/services/loader.py:213-227 realtime, :1148-1157 backfill). `update_chunk_status` (src/services/clickhouse.py:232-238) and realtime's claim/complete/fail writes (src/services/loader.py:294-300,315-322,338-347) log and continue: a failed status write leaves the old status, so a fully loaded chunk can stay `claimed`. Verify coverage from raw-table rows, never chunk status alone.
- **Permanently failing transform chunks block their loader.** Failed chunks are retried every round, oldest first, `LIMIT TRANSFORM_CHUNKS_PER_FETCH` (50) (src/services/transformer.py:246-260); any failure keeps the loop from sleeping (:167-168,215-216), and 50+ permanent failures (e.g. parse failures, :299-302) stop the loader's frontier. Read `error_message` in `transformer_progress FINAL` where `status = 'failed'`.
- **Transformer OOMKill: restart is enough.** Any chunk without a `completed` progress row, `processing` rows of any age included, is re-selected on the first poll (src/services/transformer.py:246-258); the 30-min stale sweep (:103-126) only relabels. First check `TRANSFORM_CHUNKS_PER_BATCH` is still 1: it is memory protection (src/config.py:59-62).
- **`maintain fix --force` deletes before it reloads** (src/services/maintenance.py:116 before :127; `blocks` maps 14 tables :27-36): beacon unreachable → a hole. `--dry-run` first; never widen the range. → runbook "Corrupt range".
- **`--loaders` is filtered against the pod's `ENABLED_LOADERS`**: non-enabled names are dropped; if none is enabled the filter vanishes and every loader in range is selected (src/services/maintenance.py:513-523; `reset` :681-691); `fix` cannot reload a loader it lacks (:361-374). Set `ENABLED_LOADERS` to a superset and name every loader.
- **`fix` without beacon credentials** uses `http://localhost:5052` (src/config.py:9): delete succeeds, reload fails, range left empty.
- **Two transformers or two realtime loaders double-write**: claiming is SELECT-then-INSERT (src/services/clickhouse.py:137-187), realtime writes `claimed` rows unchecked (src/services/loader.py:280-300), transform selection ignores `processing` (src/services/transformer.py:240-262), raw tables dedupe only identical payloads (migrations/003_raw_tables.sql:12-13).
- **`VALIDATOR_MODE=all_slots`** makes a 100-slot validators chunk per 100 slots, each slot a full state fetch (src/services/loader.py:586-619).
- **Never `TRUNCATE transformer_progress`** (full re-transform; use the runbook's scoped procedure) and never `OPTIMIZE … FINAL` on production (scripts/chunks.py:115 suggests it).

## Health and verification is a warehouse query

The one read-only check reproduced here, identical to the runbook and public docs "Health" query. Slot-keyed
raw tables carry a `MATERIALIZED` `slot_timestamp`, so name it (migrations/003_raw_tables.sql:8-11,22-25,
51-54,68-71,85-88,101-104; migrations/011_fork_fulu.sql:8-11):

```sql
SELECT 'raw_blocks' AS t, max(slot) AS max_slot, max(slot_timestamp) AS latest FROM consensus.raw_blocks
UNION ALL SELECT 'raw_rewards', max(slot), max(slot_timestamp) FROM consensus.raw_rewards
UNION ALL SELECT 'raw_data_column_sidecars', max(slot), max(slot_timestamp) FROM consensus.raw_data_column_sidecars
UNION ALL SELECT 'raw_validators', max(slot), max(slot_timestamp) FROM consensus.raw_validators;
```

Healthy: `raw_blocks` advances 100 slots every 8–10 min, 700–800 slots behind head
(src/services/loader.py:64-80); `raw_validators` gains one row per UTC day (src/loaders/validators.py:129-145).
Morning-check equivalent: `consensus.blocks` `max(slot_timestamp)` ≈ 67 min behind now (runbooks/00-morning-check.md:39).
Transform lag: the runbook "Transform lagging behind raw" query, per loader (the alert takes `max()` across
tables). Coverage: the runbook "Gap" `LEFT ANTI JOIN`; quick triage:
`SELECT loader_name, status, count() FROM consensus.load_state_chunks FINAL WHERE start_slot >= ⟨LO⟩ AND end_slot < ⟨HI⟩ GROUP BY loader_name, status`.

Not a health signal: a `Running` pod (no probes; an unreachable ClickHouse shows as CrashLoopBackOff, not
Running); exit 0 or a succeeded CronJob run; chunk status alone; row counts (empty slots are normal);
`/health`; `beacon_indexer_chain_lag_slots` (set only when a chunk is processed, src/services/loader.py:112,
so it freezes during a stall); `maintain check` passing.

## Rules for agents

- Derive, do not recall: verbs from src/cli.py, loaders from src/loaders/__init__.py, env from src/config.py, tables from migrations/, production values from the deployments stack — not README.md, docs/ or this file's numbers.
- **Assume the local `.env` already points at a real warehouse**: maintainer checkouts carry a cloud `CLICKHOUSE_HOST` with `CLICKHOUSE_DATABASE=consensus`. Treat any non-localhost `CLICKHOUSE_HOST` as production, including the retired pre-GKE warehouse, where a writer creates a split-brain copy.
- Never run a writer against it from a laptop: `load *`, `transform *`, every `maintain` sub-verb (`check` and `fix --dry-run` included: they run the stale-`processing` sweep), migrations, the writer `make` targets, or a writer verb via the compose `maintenance` service. Read-only SQL and `scripts/*.py` are fine.
- Never assume access to the cluster or the private deployments repository. Propose exact commands for a human to run. Never run a git write, a Terraform apply, a cluster-mutating command or an image push yourself; read-only plans and read-only cluster reads are fine where you already have access. One-shot repairs are cloned from the daily CronJob per the one-shot-jobs runbook.
- Never a second writer, never above one replica, never a `load backfill` whose loaders overlap realtime's unless its range is strictly behind realtime's frontier.
- Never: `transform reprocess`; `maintain fix --force` without `--dry-run`, beacon credentials and an `ENABLED_LOADERS` superset; `maintain reset --status completed`; `VALIDATOR_MODE=all_slots`; raising `TRANSFORM_CHUNKS_PER_BATCH`; `TRUNCATE transformer_progress`; `OPTIMIZE … FINAL`; state tables without `FINAL`; `validators` on a non-archive node.
- Slot bounds: `load backfill` `--end-slot` is exclusive (src/services/loader.py:554); `maintain` selects `start_slot >= N AND end_slot <= M` (src/services/maintenance.py:500-503); use 100-slot boundaries.
- Schema changes: a new numbered `migrations/NNN_*.sql` with `IF NOT EXISTS`; never edit an applied file (scripts/migrate.py:27-41 re-runs all). Ship it via the one-shot `python -m scripts.migrate` Job above, before pinning the image that needs it.
- Tests: `pip install -r requirements.txt -r requirements-dev.txt && python -m pytest` (requirements-dev.txt:1-4 has test deps only), after `export CLICKHOUSE_HOST=localhost` — `load_dotenv` (src/config.py:5) does not override variables already set. No `make test`.
- Lessons: no `.agents/` or `docs/lessons/` here. Operational lessons go in the private runbook (and the public docs page if safe); code-level traps as a bullet here with `path:line`.

## Where the full procedures live

- README.md — local compose quick start and `maintain` reference; some Makefile target names are fictional (above).
- docs/*.md — design, env reference, local runs, adding a loader; defaults lag src/config.py. docs/pending-queues.md — the Electra `pending_*` design.
- Private runbook (second source of truth): https://github.com/gnosisdevops/infrastructure-gnosis-analytics/blob/main/runbooks/21-beacon-indexer.md — stopped/gap/corrupt-range/re-derive/transform-lag/safe-stop-and-restart procedures and the `[danger]` list.
- Public docs page: https://docs.analytics.gnosis.io/data-pipeline/ingestion/beacon-indexer/ — the same procedures without cluster identifiers, plus schemas; one-shot Jobs: https://docs.analytics.gnosis.io/operations/runbooks/one-shot-jobs/.
- After a raw repair (runbook 21 has no dbt step; the public page's "Then dbt" does): https://github.com/gnosisdevops/infrastructure-gnosis-analytics/blob/main/runbooks/31-dbt-reprocess-after-upstream-repair.md and https://docs.analytics.gnosis.io/operations/runbooks/dbt-reprocess/.
- Deployments stack: the beacon-indexer stack in the private repository's GKE tree — workloads, `command`, env, resources, schedule, and what was not ported.

Verified 2026-09-23 against: src/cli.py:15-366, src/main.py:1-10, src/config.py:1-74, src/services/loader.py:21-374,408-724,997-1209, src/services/transformer.py:97-340,570-618, src/services/maintenance.py:20-138,207-374,495-715, src/services/beacon_api.py:44-227, src/services/storage_factory.py:1-14, src/services/clickhouse.py:1-245,311-388,429-834, src/services/fork.py:72-74,147-165,405-445, src/loaders/__init__.py:1-33, src/loaders/base.py:1-106, src/loaders/validators.py:129-145,193-284, src/loaders/genesis.py:24-107, src/loaders/specs.py:1-91, src/loaders/data_column_sidecars.py:16-20, src/loaders/pending_consolidations.py:1-41, src/loaders/pending_deposits.py:15-30, src/observability.py:146-177, docker-entrypoint.sh:1-35, Dockerfile:1-27, .github/workflows/build-and-release.yml:1-57, Makefile:1-145, docker-compose.yml:1-152, scripts/migrate.py:1-66, scripts/chunks.py:1-163, scripts/maintenance.py:87-150,416-442, scripts/transformer_status.py:16-80, migrations/003_raw_tables.sql:1-107, migrations/004_state_tables.sql:1-28, migrations/011_fork_fulu.sql:1-14, migrations/012_pending_queues.sql:1-20, config/forks.yaml:1-37, README.md:96-102,166-184,406, docs/CONFIGURATION.md:22-26,42,52, docs/DEVELOPMENT.md:25-26,245,572, docs/PARQUET_GUIDE.md:104-126, docs/STORAGE_BACKEND.md:46-57, .env.example:30,49, .env (key patterns only, no values), .gitignore:8, requirements.txt:1-10, requirements-dev.txt:1-4, tests/*.py imports; deployments stack locals.tf:15-89, deployments.tf:1-145, cronjob.tf:15-161; runbooks/21-beacon-indexer.md:1-210, runbooks/00-morning-check.md:39, runbooks/31-dbt-reprocess-after-upstream-repair.md (exists); public docs page "Health", "Then dbt" and "ClickHouse Settings" sections, operations/runbooks/dbt-reprocess (exists)
