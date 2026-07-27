# OmniRoute Copier Production Transition Roadmap

## Objective

Move the trade copier from a single-process, MT5-terminal-switching design to a production model where:

- one master can feed up to five slaves
- each slave executes in isolation
- trade signals are durable and replayable
- broker state is reconciled after execution
- failures are visible, classified, and recoverable

## Implementation Status

- Step 1: completed
- Step 2: completed
- Step 3: pending
- Step 4: pending
- Step 5: pending
- Step 6: pending
- Step 7: pending
- Step 8: pending
- Step 9: pending
- Step 10: pending

This roadmap is scoped only to the trade copier. It does not cover the landing page, auth UI, or bot strategy UX unless they directly affect copier execution.

---

## Current Copier State

The current codebase already does the following:

- accepts trade intents via FastAPI
- routes master signals by `magic_number`
- resolves linked slaves from SQLite
- applies symbol mapping
- applies per-slave slippage and risk settings
- translates SL/TP
- sends MT5 orders
- records slave positions and modify logs
- supports simulation mode when MT5 is unavailable
- now persists trade intents and per-slave trade jobs

The current limitation is structural:

- MT5 execution is still too coupled to the request path
- a single process is effectively coordinating account context
- master and slave concerns are not fully isolated
- there is no durable queue-backed execution boundary yet

That is acceptable for development and controlled testing, but not for production.

---

## Target Production Shape

The copier should be split into four layers:

1. **Control plane**
   - FastAPI API
   - authentication
   - account management
   - trade intent ingestion
   - status and reconciliation visibility

2. **Durable execution queue**
   - persisted signal intents
   - persisted per-slave jobs
   - retries
   - replay after restart

3. **MT5 worker layer**
   - one worker process per terminal/account context
   - one worker owns one MT5 terminal session
   - no shared execution terminal across unrelated accounts

4. **Reconciliation and audit layer**
   - confirm what actually happened at the broker
   - repair internal state after partial failures
   - preserve an auditable timeline

---

## Production State Model

The copier should treat every request as a transition between explicit states.

### 1. Intent states

These describe the lifetime of the incoming trade event.

- `received`
  - request arrived
  - not yet validated or dispatched

- `queued`
  - intent persisted
  - target resolution is pending or in progress

- `accepted`
  - intent validated
  - eligible slave jobs have been created or are being created

- `no_targets`
  - no eligible slaves were connected
  - nothing was executed

- `rejected`
  - request failed validation or routing
  - no broker execution should occur

- `completed`
  - all eligible jobs finished successfully

- `completed_with_errors`
  - at least one job failed or was blocked
  - intent finished, but not all targets succeeded

### 2. Job states

Each slave gets its own job row.

- `queued`
  - job created
  - not yet handed to a worker

- `dispatching`
  - assigned to a worker
  - waiting for MT5 execution

- `executing`
  - worker is calling broker logic

- `confirmed`
  - broker action succeeded and was verified

- `blocked`
  - execution rejected by risk controls
  - no broker order should be assumed live

- `failed`
  - execution failed due to broker or runtime error

- `retry_wait`
  - temporary failure occurred
  - job is scheduled for a later attempt

- `dead_letter`
  - retry limit exceeded
  - manual intervention required

### 3. Worker states

Each MT5 worker owns one account context.

- `starting`
  - process booting
  - terminal health not yet confirmed

- `idle`
  - ready for work

- `busy`
  - currently executing a job

- `degraded`
  - worker is alive but unstable
  - broker connectivity or reconciliation failed

- `offline`
  - worker or terminal unavailable

- `draining`
  - worker is being taken out of rotation
  - finishes current job, then stops accepting new ones

### 4. Terminal states

The MT5 terminal itself needs state tracking.

- `uninitialized`
  - terminal process not attached

- `attached`
  - API is connected to terminal IPC

- `logged_in`
  - terminal is authenticated to an account

- `switching`
  - login/account change in progress

- `error`
  - MT5 IPC or account login failed

### 5. Reconciliation states

These describe whether internal state matches broker reality.

- `pending`
  - reconciliation not yet run

- `matched`
  - internal and broker state agree

- `mismatch`
  - broker state and internal state differ

- `orphaned`
  - broker position exists with no matching internal record

- `repaired`
  - discrepancy was corrected or annotated

---

## Step-by-Step Transition Plan

## Phase 1: Freeze the current execution contract

Goal: stop the copier from changing behavior while the new architecture is being introduced.

### Tasks

- Keep the existing signal format stable.
- Keep `magic_number` as the master routing key.
- Keep symbol mapping and risk configuration behavior unchanged.
- Add journal rows for every accepted intent and every job.
- Stop adding new execution features until the queue and worker model exists.

### Deliverables

- documented current contract
- stable API payloads
- explicit intent/job tables
- clear success and failure states

### Why this matters

If the execution contract keeps moving during the architecture change, you will not be able to tell whether a failure came from the new worker model or from a request-shape regression.

---

## Phase 2: Move to durable intent + job separation

Goal: make every trade event durable before any MT5 action.

### What happens now

The API receives a signal and immediately routes it in-process.

### What should happen

1. API receives signal.
2. API validates basic shape.
3. API writes a trade intent row.
4. API resolves candidate slaves.
5. API creates one job row per eligible slave.
6. Execution is handed off to workers.

### Technical details

- Keep `trade_intents`.
- Keep `trade_jobs`.
- Add `trade_job_attempts`.
- Add `broker_reconciliations`.
- Include these fields in the intent:
  - `intent_id`
  - `idempotency_key`
  - `intent_type`
  - `master_id`
  - `magic_number`
  - `symbol`
  - `direction`
  - payload JSON
  - status
  - timestamps

### Acceptance criteria

- A signal can be persisted even if the worker layer is down.
- Duplicate retries do not create duplicate live orders.
- Every open/close/modify request has a durable audit trail.

---

## Phase 3: Introduce a real queue boundary

Goal: decouple API latency from MT5 execution latency.

### Problem being solved

MT5 broker actions are slow, stateful, and brittle. They should not happen in the request handler.

### Recommended options

#### Option A: Postgres-backed queue

Best if you want fewer moving parts.

- use database rows as the source of truth
- workers poll for pending jobs using row locks
- good fit if you already want Postgres for durability

#### Option B: Redis + worker system

Best if you want faster dispatch and easier concurrency.

- use Redis as the transient queue
- persist state in Postgres
- good fit for higher throughput

#### Option C: Lightweight custom worker poller

Best if you want the smallest initial change.

- API writes jobs to DB
- worker loops poll DB for runnable jobs
- simpler to implement, slower to scale

### Recommendation

For this project, start with a Postgres-backed queue or a DB poller.
That is the simplest way to make the execution durable without introducing a second infrastructure tier too early.

### Worker contract

Each worker should:

- claim one job at a time
- execute it
- record the attempt
- record the final status
- release the job

No worker should assume it can process another account’s terminal session.

---

## Phase 4: Isolate MT5 execution by worker

Goal: avoid the master/slave collision problem by making terminal ownership explicit.

### The collision you are seeing

One MT5 instance is being used as if it can represent:

- the master account
- one or more slave accounts
- a shared broker session for all of them

That approach fails in production because MT5 is not a stateless multi-account broker fabric.

### Production rule

- one worker = one MT5 terminal = one account context

### Practical implementation

If you need:

- 1 master
- up to 5 slaves

then use:

- 1 signal source for the master
- up to 5 slave workers
- 1 terminal session per slave worker
- optional separate terminal session for the master if the master also trades live

### Two supported topologies

#### Topology 1: Signal-only master

- master EA publishes signals only
- it does not execute orders on the same terminal used for slaves
- slave workers own all execution

This is the preferred production topology.

#### Topology 2: Live-trading master

- master also trades in MT5
- master runs in a separate terminal process
- slave workers still own their own terminals

This is the safest topology if the master must be a live account.

### Why this is necessary

MT5 IPC is process-bound and stateful.
If the same terminal is used to bounce between accounts, you get:

- account switching delays
- login instability
- false assumptions about execution state
- race conditions under concurrent fan-out

---

## Phase 5: Build the worker lifecycle

Goal: make each worker observable and recoverable.

### Worker startup

1. process starts
2. terminal process is checked
3. IPC connection is validated
4. account login is validated
5. worker moves to `idle`

### Worker execution

1. worker claims a job
2. worker loads job payload
3. worker validates current terminal/account state
4. worker performs slippage and risk checks
5. worker sends broker request
6. worker records result
7. worker reconciles if needed

### Worker shutdown

1. worker enters `draining`
2. current job completes or is safely aborted
3. no new jobs are accepted
4. terminal is detached or left in a known state

### Required worker metadata

- worker id
- terminal path
- account id
- broker server
- current state
- last heartbeat
- current job id
- last error

---

## Phase 6: Add reconciliation as a first-class step

Goal: verify the broker did what the app believes happened.

### Why reconciliation matters

MT5 calls can succeed, partially succeed, or succeed after an IPC timeout.
If you only trust the request response, you will eventually drift from broker reality.

### Reconciliation checks

For open jobs:

- verify the expected ticket exists
- verify symbol matches
- verify lot size matches
- verify SL/TP match within tolerance

For close jobs:

- verify position is closed
- verify no lingering partial position remains

For modify jobs:

- verify SL/TP are updated
- verify the position still belongs to the intended magic number

### Reconciliation outcomes

- `matched`
- `mismatch`
- `orphaned`
- `repaired`

### Data to persist

- expected state
- actual broker state
- status
- notes
- timestamp

---

## Phase 7: Add retries and failure classification

Goal: retry transient failures, but never blindly duplicate trade intent.

### Failure classes

#### Transient

- MT5 IPC not ready
- temporary network failure
- worker timeout
- broker busy

These can be retried.

#### Terminal/account failure

- login failed
- wrong account attached
- terminal unavailable

These need worker repair before retrying.

#### Permanent

- slippage exceeded and action is cancel
- symbol invalid
- risk guard rejected trade
- account disabled

These should not be retried automatically.

### Retry policy

- retry count per job
- exponential backoff
- bounded maximum attempts
- dead-letter status after exhaustion

### Important rule

Retries must be job-scoped, not intent-scoped.

That means:

- one master signal can fan out to five jobs
- one job can retry without re-opening the other four

---

## Phase 8: Add hard safety limits

Goal: prevent the copier from overtrading or drifting into unsafe states.

### Limits to enforce

- max 1 master signal source per master `magic_number`
- max 5 slave accounts per master group
- max open trades per slave
- max exposure per slave
- max exposure per master
- stale connection timeout
- duplicate link prevention
- duplicate execution prevention

### Policy decisions

- if a slave is disconnected, it should not be silently skipped forever without audit
- if a job is blocked by risk, that must be visible in status
- if a master has no eligible slaves, the intent should still be recorded as `no_targets`

---

## Phase 9: Replace the in-memory execution assumption with operational truth

Goal: stop depending on memory as the source of execution truth.

### Current problem

The app still keeps active masters, slaves, and counters in memory.
That is acceptable for UI state, but not for authoritative execution history.

### New rule

Persistent rows are the source of truth for:

- signal acceptance
- job status
- attempt history
- reconciliation history

Memory is only for:

- live connection cache
- worker heartbeat
- transient execution optimization

---

## Phase 10: Observability and audit

Goal: make failures explainable.

### Required telemetry

- accepted intents
- created jobs
- blocked jobs
- failed jobs
- retried jobs
- confirmed jobs
- reconciliation mismatches
- worker heartbeat
- terminal status

### Required metrics

- signals received
- jobs created
- jobs executed
- jobs blocked
- jobs failed
- jobs retried
- average execution latency
- worker online count
- slave online count

### Required logs

- per-intent trace id
- per-job trace id
- per-worker id
- broker retcode
- account id
- magic number
- symbol
- latency

---

## Recommended Technical Stack

### Control plane

- Python
- FastAPI
- Pydantic
- PostgreSQL
- SQLAlchemy or SQLModel

### Queue and execution

- DB-backed job queue for the first production iteration
- later option: Redis or Celery if throughput requires it
- Python worker processes

### Broker integration

- MetaTrader 5 terminal per worker
- one terminal process per slave account context

### Ops

- Windows service wrapper or process supervisor on the VPS
- structured file logs
- heartbeat endpoint
- health probes
- automatic restart on worker crash

### Optional later-stage pieces

- Redis for dispatch and pub/sub
- Prometheus for metrics
- Grafana for dashboards
- Sentry or equivalent for error aggregation

---

## Suggested Build Order

### Milestone 1

- finish intent and job persistence
- keep current execution path working

### Milestone 2

- introduce worker claims and queue polling
- move execution out of the request thread

### Milestone 3

- isolate each slave to one worker and one terminal session

### Milestone 4

- add reconciliation and retry logic

### Milestone 5

- add safety caps and duplicate-prevention rules

### Milestone 6

- run the system under load with one master and up to five slaves

### Milestone 7

- cut over production traffic
- keep a rollback path to the older execution mode until the new path proves stable

---

## Cutover Strategy

Do not switch directly from the current design to full production mode.

Use a staged cutover:

1. **Shadow mode**
   - intents and jobs are recorded
   - worker execution is simulated or mirrored

2. **Single-slave production trial**
   - one master, one slave
   - validate end-to-end behavior

3. **Limited fan-out**
   - one master, two or three slaves
   - verify timing, retries, and reconciliation

4. **Full target fan-out**
   - one master, up to five slaves
   - monitor stability and drift

5. **Operational hardening**
   - adjust retry thresholds
   - tune timeouts
   - tighten safety limits

---

## Final Guidance

The production answer for this project is not “make one MT5 instance do everything.”

The production answer is:

- control plane in FastAPI
- durable job state in PostgreSQL
- one MT5 worker per slave account
- one terminal per worker
- master as a signal source or isolated terminal
- reconciliation after every meaningful action

That is the clean design if you want one master feeding up to five slaves without MT5 session collisions.
