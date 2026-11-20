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

- The **same Python minor version** as your laptop. The agent classes are defined in the script itself, so they are serialized as code, and that doesn't work across Python versions.
- **On a multi-user endpoint, the same Python *and* `globus-compute-endpoint` version as the endpoint itself.** The workers started from your environment must match the endpoint's task-dispatch process exactly (Python version and the Parsl version that `globus-compute-endpoint` pins), or every task fails with `Manager version info ... does not match interchange version info`. You can look up the endpoint's versions with `Client().get_endpoint_metadata(ENDPOINT_ID)` (`python_version`, `endpoint_version`).
- `academy-py` (ideally the same version as your laptop), `numpy`, `scikit-image`, `psutil`.
- Outbound HTTPS to `exchange.academy-agents.org`. At ALCF, compute nodes use `proxy.alcf.anl.gov:3128` for this. On Polaris `http(s)_proxy` is already set in jobs, but exporting it in `worker_init` does no harm.

For the Polaris multi-user endpoint `9a947ba5-f537-4681-acf3-cc66485aadec` (Python 3.13, Globus Compute 4.9.0), set up the worker environment on a login node with:

```bash
uv venv ~/src/Academy/recon-agent-venv --python 3.13 --managed-python  # tomo_agent.py is pinned to 3.13
source ~/src/Academy/recon-agent-venv/bin/activate
uv pip install academy-py==1.0.0 globus-compute-endpoint==4.9.0 numpy scikit-image psutil
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

### 3. On a multi-user endpoint (e.g. ALCF Polaris)

Multi-user endpoints take a per-user config, typically a PBS account, queue, walltime and a `worker_init` that prepares the environment. The keys come from the endpoint's template; `Client().get_endpoint_metadata(ENDPOINT_ID)["user_config_schema"]` lists them.

This is the command tested on the Polaris multi-user endpoint (`account` and `queue` are required there):

```bash
uv run tomo_agent.py --endpoint 9a947ba5-f537-4681-acf3-cc66485aadec --user-endpoint-config '{
  "account": "APSDataAnalysis",
  "queue": "debug",
  "walltime": "00:30:00",
  "worker_init": "source ~/src/Academy/recon-agent-venv/bin/activate"
}'
```

Things to know:

- The "agent started" notification only arrives once the PBS job starts, so the first run can sit in the queue for a while (about 25 minutes in testing on the `debug` queue). `--start-timeout` (default 1800 s) controls how long the script waits. If the agent crashes on startup, the script exits immediately with the remote error.
- The endpoint keeps the PBS job for about 4 minutes after it goes idle. A second run with the **identical** `--user-endpoint-config` reuses the same node and starts in seconds; any change to the config starts a new job.
- The agent writes to the node-local `/tmp` by default, which is wiped when the job ends. Pass `--workdir` with a path on a shared filesystem (e.g. under your home directory) to keep the log and output.

### Options

| Flag | Default | Meaning |
|---|---|---|
| `--local` / `--endpoint UUID` | (one required) | Run in-process, or on a Globus Compute endpoint |
| `--user-endpoint-config JSON` | none | Per-user config for a multi-user endpoint |
| `--workdir DIR` | remote temp dir | Where the log and output `.npy` are written on the remote machine |
| `--slices` / `--size` / `--angles` | 128 / 512 / 360 | Size of the reconstruction (about 90 s on a laptop, 210 s on a Polaris node; single-threaded) |
| `--poll SECONDS` | 3 | Time between progress/utilization checks |
| `--cancel-after SECONDS` | none | Cancel the reconstruction after this long, to demo cancellation |
| `--start-timeout SECONDS` | 1800 | How long to wait for the agent to report it started |

## Example output

From the Polaris multi-user endpoint, with `--cancel-after 30`:

```
Submitted TomoAgent, waiting for it to start (on a batch-scheduled MEP this can take a while)...

🔔  [agent-started] {"agent_id": "AgentId<7fbabdc8>", "hostname": "x3001c0s19b0n0", "pbs_job_id": "7869140.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov", "pid": 2276953, "cpus": 64, "log": "/tmp/tomo-7fbabdc8-5e0e-4f29-9fe4-bce3bdd9427f.log"}

Agent is running on x3001c0s19b0n0 (PBS job: 7869140.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov)
[   0.6s]      idle    0/0    (  0.0%) | cpu   0.0%  load  0.81  mem  3.3%  @ x3001c0s19b0n0
[   6.1s]   running    2/128  (  1.6%) | cpu   1.6%  load  0.83  mem  3.4%  @ x3001c0s19b0n0
[  12.1s]   running    6/128  (  4.7%) | cpu   1.6%  load  0.84  mem  3.4%  @ x3001c0s19b0n0
...
[  34.3s]   running   19/128  ( 14.8%) | cpu   1.6%  load  0.90  mem  3.3%  @ x3001c0s19b0n0
Cancelling reconstruction...

🔔  [reconstruction-done] {"state": "cancelled", "slices_done": 21, "elapsed_s": 34.2}

Log on remote machine: /tmp/tomo-7fbabdc8-5e0e-4f29-9fe4-bce3bdd9427f.log
```

Without `--cancel-after`, the full 128-slice reconstruction took 209 s on a Polaris node. CPU shows about 1.6% because the example reconstruction uses one of the node's 64 cores.

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

- **Import inside the agent's methods.** Because `TomoAgent` is defined in the script, Globus Compute sends it to the remote machine as code, but the script's top-level imports don't go with it. A method that uses a module imported only at the top of the file fails remotely with `NameError` (while working fine with `--local`). That's why every `TomoAgent` method has its own imports. Moving the agent into an installed module, imported on both sides, also avoids this.
- **The agent occupies a Globus Compute worker for its whole lifetime.** On a batch-scheduled endpoint that means a job, so walltime limits how long the agent can live.
- **Cancellation is cooperative.** Python threads can't be killed, so cancel takes effect between slices. If the reconstruction is an external program, run it with `asyncio.create_subprocess_exec` and have `cancel()` call `proc.terminate()`.
- **Authentication.** `HttpExchangeFactory(auth_method='globus')` includes your Globus access token in the exchange settings sent to the remote agent, so the agent talks to the exchange as you. That's fine for a demo. For per-agent identities, see Academy's `GlobusExchangeFactory` (`examples/12-globus-exchange` in the Academy repo).
- **CPU use is low** because the example reconstruction is single-threaded. Parallelizing across slices (or using tomopy) will make the utilization numbers more interesting.

## Next step: adding an LLM

A natural v2 is a third agent, backed by an LLM, that holds the `Handle[TomoAgent]` and uses its actions as tools. A `@timer` loop could feed it progress and utilization, and it could decide to cancel and restart with different parameters (for example if memory climbs or progress stalls). `TomoAgent` itself wouldn't need to change. See `docs/guides/llm.md` and `examples/06-llm` in the Academy repo.
