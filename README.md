# Remote tomography with Academy + Globus Compute

A single-file example (`tomo_agent.py`) showing how an [Academy](https://academy-agents.org) agent can be deployed to an HPC resource with [Globus Compute](https://www.globus.org/compute) to run a tomographic reconstruction. While the reconstruction runs, you can monitor and control it from your laptop:

- start a reconstruction
- check its progress (read from the reconstruction log)
- check CPU, memory and load on the machine
- ask which host the agent is running on
- cancel the reconstruction
- get a notification when the agent starts, including which machine it is on

No LLM is involved. The agent is plain Python driven by a script.

## How it works

```
 laptop (tomo_agent.py)                         HPC compute node
┌───────────────────────┐                      ┌──────────────────────┐
│ Manager               │  Globus Compute      │ Globus Compute worker│
│  ├─ Notifier agent  ◄─┼──── notify() ────────┤  └─ TomoAgent        │
│  └─ Handle[TomoAgent]─┼──── actions ────────►│      ├─ reconstruct  │
└───────────────────────┘                      │      ├─ get_progress │
            ▲                                  │      ├─ get_utilization
            │   all messages go through the    │      ├─ get_hostname │
            │   hosted Academy exchange        │      └─ cancel       │
            └── exchange.academy-agents.org ──►└──────────────────────┘
```

There are two agents:

**`TomoAgent`** runs on the remote machine. Its methods marked `@action` can be called from anywhere through a *handle*:

| Action | What it does |
|---|---|
| `reconstruct(n_slices, size, n_angles)` | Builds a synthetic 3D phantom, simulates a scan of each slice and reconstructs it with filtered back-projection. Writes `slice i/N` lines to a log file. Returns when the reconstruction finishes or is cancelled. |
| `get_progress()` | Parses the log and returns the state (`preparing`/`running`/`finished`/`cancelled`/`failed`), slices done and percent complete. |
| `get_utilization()` | CPU %, load average and memory for the host, plus the agent process's memory use (via `psutil`). |
| `get_hostname()` | The host the agent is executing on. |
| `cancel()` | Asks a running reconstruction to stop after the current slice. |

**`Notifier`** runs inside your local script. The remote agent gets a handle to it, and in `agent_on_startup` it calls `notifier.notify('agent-started', {...})` with its hostname, PID, CPU count and log path. The driver also uses it to announce when the reconstruction is done.

### Why you can monitor while it runs

Academy runs each incoming action as its own asyncio task, so actions don't wait for each other. The reconstruction is blocking numerical code, so it runs in a thread via `agent_run_sync`. That leaves the agent's event loop free to answer `get_progress`, `get_utilization` and `cancel` in the middle of a reconstruction. On the client side, the driver starts `reconstruct()` as a background task and polls the other actions until it completes.

### How the agent gets to the remote machine

The `Manager` is given a Globus Compute `Executor`. `manager.launch(TomoAgent, executor='globus_compute')` submits a function to the endpoint that starts the agent and keeps it running until the Manager shuts it down. When the script leaves the `async with Manager...` block, the remote agent is shut down.

## Requirements

**On your laptop:** [uv](https://docs.astral.sh/uv/). Dependencies are declared inline in `tomo_agent.py`, so `uv run` installs them for you. If you're not using uv: `pip install academy-py globus-compute-sdk numpy scikit-image psutil`.

**On the Globus Compute endpoint's workers:**

- The **same Python minor version** as your laptop (e.g. both 3.12). The agent classes are defined in the script itself, so they are serialized as code, and that doesn't work across Python versions.
- `academy-py` (ideally the same version as your laptop), `numpy`, `scikit-image`, `psutil`.
- Outbound HTTPS to `exchange.academy-agents.org`. At ALCF, compute nodes need the proxy for this:
  `export HTTPS_PROXY=http://proxy.alcf.anl.gov:3128 https_proxy=http://proxy.alcf.anl.gov:3128`

A simple way to set up the worker environment on the HPC system:

```bash
uv venv ~/tomo-venv --python 3.12        # match your laptop's Python
source ~/tomo-venv/bin/activate
uv pip install academy-py numpy scikit-image psutil globus-compute-endpoint
```

## Running it

### 1. Locally (no Globus Compute, no login)

Runs both agents in one process with an in-memory exchange. Use this to check that everything works before going remote.

```bash
uv run tomo_agent.py --local
```

### 2. On your own Globus Compute endpoint

Start an endpoint on the HPC system (on a login node it will run agents there; configure a batch provider to run on compute nodes):

```bash
globus-compute-endpoint configure tomo
globus-compute-endpoint start tomo       # prints the endpoint UUID
```

Then, from your laptop:

```bash
uv run tomo_agent.py --endpoint <ENDPOINT_UUID>
```

The first run opens a Globus login for the hosted Academy exchange (and for Globus Compute, if you haven't logged in before).

### 3. On a multi-user endpoint (e.g. ALCF)

Multi-user endpoints take a per-user config, typically a PBS account, queue, walltime and a `worker_init` that prepares the environment. The exact keys depend on how the endpoint's template is written; ask the endpoint admin or check its documentation.

```bash
uv run tomo_agent.py --endpoint <MEP_UUID> --user-endpoint-config '{
  "account": "MyProject",
  "queue": "debug",
  "walltime": "00:30:00",
  "worker_init": "source ~/tomo-venv/bin/activate; export HTTPS_PROXY=http://proxy.alcf.anl.gov:3128 https_proxy=http://proxy.alcf.anl.gov:3128"
}'
```

The "agent started" notification only arrives once the batch job is scheduled, so this can take a while. `--start-timeout` (default 1800 s) controls how long the script waits.

### Options

| Flag | Default | Meaning |
|---|---|---|
| `--local` / `--endpoint UUID` | (one required) | Run in-process, or on a Globus Compute endpoint |
| `--user-endpoint-config JSON` | none | Per-user config for a multi-user endpoint |
| `--workdir DIR` | remote temp dir | Where the log and output `.npy` are written on the remote machine |
| `--slices` / `--size` / `--angles` | 128 / 512 / 360 | Size of the reconstruction (about 90 s single-threaded on a laptop) |
| `--poll SECONDS` | 3 | Time between progress/utilization checks |
| `--cancel-after SECONDS` | none | Cancel the reconstruction after this long, to demo cancellation |
| `--start-timeout SECONDS` | 1800 | How long to wait for the agent to report it started |

## Example output

```
Submitted TomoAgent, waiting for it to start (on a batch-scheduled MEP this can take a while)...

🔔  [agent-started] {"agent_id": "AgentId<a2513a14>", "hostname": "x4000c0s0b0n0", "pid": 15748, "cpus": 208, "log": "/tmp/tomo-a2513a14-....log"}

Agent is running on x4000c0s0b0n0
[   0.5s] preparing    0/0    (  0.0%) | cpu   0.0%  load  0.42  mem  3.8%  @ x4000c0s0b0n0
[   3.0s]   running    3/128  (  2.3%) | cpu  11.6%  load  0.42  mem  4.1%  @ x4000c0s0b0n0
...
[  15.6s]   running   20/128  ( 15.6%) | cpu   9.9%  load  0.95  mem  3.7%  @ x4000c0s0b0n0
Cancelling reconstruction...

🔔  [reconstruction-done] {"state": "cancelled", "slices_done": 21, "elapsed_s": 15.4}
```

(The hostnames above are illustrative; the cancel sequence was captured from a local run.)

## Using it from your own code

The driver in `main()` is only one way to use the agent. Once you have a handle, any action can be called at any time:

```python
tomo = await manager.launch(TomoAgent, args=(notifier, None), executor='globus_compute')

job = asyncio.create_task(tomo.reconstruct(n_slices=256))
print(await tomo.get_hostname())
print(await tomo.get_progress())
print(await tomo.get_utilization())
await tomo.cancel()
print(await job)
```

To use real data, replace the body of `TomoAgent._reconstruct_blocking` (for example with tomopy). Keep the two parts the monitoring relies on: write `slice i/N` lines to `self.log_path`, and check `self._cancel` between units of work.

## Caveats

- **The agent occupies a Globus Compute worker for its whole lifetime.** On a batch-scheduled endpoint that means a job, so walltime limits how long the agent can live.
- **Cancellation is cooperative.** Python threads can't be killed, so cancel takes effect between slices. If the reconstruction is an external program, run it with `asyncio.create_subprocess_exec` and have `cancel()` call `proc.terminate()`.
- **Authentication.** `HttpExchangeFactory(auth_method='globus')` includes your Globus access token in the exchange settings sent to the remote agent, so the agent talks to the exchange as you. That's fine for a demo. For per-agent identities, see Academy's `GlobusExchangeFactory` (`examples/12-globus-exchange` in the Academy repo).
- **CPU use is low** because the example reconstruction is single-threaded. Parallelizing across slices (or using tomopy) will make the utilization numbers more interesting.

## Next step: adding an LLM

A natural v2 is a third agent, backed by an LLM, that holds the `Handle[TomoAgent]` and uses its actions as tools. A `@timer` loop could feed it progress and utilization, and it could decide to cancel and restart with different parameters (for example if memory climbs or progress stalls). `TomoAgent` itself wouldn't need to change. See `docs/guides/llm.md` and `examples/06-llm` in the Academy repo.
