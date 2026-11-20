# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "academy-py",
#     "globus-compute-sdk",
#     "numpy",
#     "scikit-image",
#     "psutil",
# ]
# ///
"""Remote tomography reconstruction agent with Academy + Globus Compute.

A ``TomoAgent`` is deployed to an HPC resource (e.g. an ALCF machine) via a
Globus Compute endpoint. From your laptop you can then, *concurrently*:

* start a reconstruction            -> ``await agent.reconstruct(...)``
* watch progress (parsed from a log) -> ``await agent.get_progress()``
* watch machine utilization          -> ``await agent.get_utilization()``
* ask where it is running            -> ``await agent.get_hostname()``
* cancel the reconstruction          -> ``await agent.cancel()``

When the agent starts on the remote machine it calls back to a small
``Notifier`` agent running inside this script, so you are told when (and
where) it came up.

How the pieces fit:

    laptop (this script)                          ALCF compute node
    ┌──────────────────────┐                     ┌─────────────────────┐
    │ Manager              │  GC executor.submit │ Globus Compute      │
    │  ├─ Notifier agent ◄─┼──── notify() ───────┤  worker             │
    │  └─ Handle[TomoAgent]┼──── actions ───────►│   └─ TomoAgent      │
    └──────────────────────┘                     └─────────────────────┘
               ▲    all messages go via the hosted Academy exchange    ▲
               └──────────── https://exchange.academy-agents.org ──────┘

Usage:
    # 1. Try it locally first (no Globus Compute, no auth, in-process exchange)
    uv run tomo_agent.py --local

    # 2. Run on a Globus Compute endpoint (your own, or an ALCF multi-user
    #    endpoint). The endpoint's worker environment needs the same Python
    #    minor version as here, plus: academy-py numpy scikit-image psutil
    uv run tomo_agent.py --endpoint <ENDPOINT_UUID>

    # 3. Multi-user endpoint (MEP) that needs a PBS account/queue, etc.
    uv run tomo_agent.py --endpoint <MEP_UUID> \
        --user-endpoint-config '{"account": "MyProject", "queue": "debug",
                                 "walltime": "00:30:00",
                                 "worker_init": "source ~/tomo-venv/bin/activate; export HTTPS_PROXY=http://proxy.alcf.anl.gov:3128 https_proxy=http://proxy.alcf.anl.gov:3128"}'

    (ALCF compute nodes need the proxy to reach the hosted Academy exchange.
    The exact template keys depend on how the MEP's template is written.)

    # Demo cancellation: cancel the reconstruction after 20 seconds
    uv run tomo_agent.py --endpoint <ENDPOINT_UUID> --cancel-after 20
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import pathlib
import re
import socket
import tempfile
import threading
import time
from typing import Any

from academy.agent import action
from academy.agent import Agent
from academy.handle import Handle
from academy.manager import Manager

logger = logging.getLogger('tomo')


# --------------------------------------------------------------------------- #
# Notifier: runs locally, inside this script. The remote agent calls it.
# --------------------------------------------------------------------------- #
class Notifier(Agent):
    """Receives one-way notifications from remote agents."""

    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.info: dict[str, Any] = {}

    @action
    async def notify(self, event: str, info: dict[str, Any]) -> None:
        """Print a notification sent by a remote agent."""
        print(f'\n🔔  [{event}] {json.dumps(info)}\n', flush=True)
        if event == 'agent-started':
            self.info = info
            self.started.set()

    @action
    async def wait_started(self, timeout: float = 600) -> dict[str, Any]:
        """Block until a remote agent has reported that it started."""
        await asyncio.wait_for(self.started.wait(), timeout)
        return self.info


# --------------------------------------------------------------------------- #
# TomoAgent: runs remotely, on the HPC resource.
# --------------------------------------------------------------------------- #
class TomoAgent(Agent):
    """Runs a tomographic reconstruction and reports on it while it runs."""

    def __init__(self, notifier: Handle[Notifier], workdir: str | None = None):
        super().__init__()
        self.notifier = notifier
        self.workdir = workdir
        self.log_path: pathlib.Path | None = None
        self._cancel = threading.Event()
        self._running = False

    # -- lifecycle ----------------------------------------------------------
    async def agent_on_startup(self) -> None:
        # Runs on the remote machine as soon as the agent is up.
        base = pathlib.Path(self.workdir or tempfile.gettempdir())
        base.mkdir(parents=True, exist_ok=True)
        self.log_path = base / f'tomo-{self.agent_id.uid}.log'
        await self.notifier.notify(
            'agent-started',
            {
                'agent_id': str(self.agent_id),
                'hostname': socket.gethostname(),
                'pid': os.getpid(),
                'cpus': os.cpu_count(),
                'log': str(self.log_path),
            },
        )

    # -- actions ------------------------------------------------------------
    @action
    async def get_hostname(self) -> str:
        """Hostname of the machine the agent is executing on."""
        return socket.gethostname()

    @action
    async def reconstruct(
        self,
        n_slices: int = 128,
        size: int = 512,
        n_angles: int = 360,
    ) -> dict[str, Any]:
        """Simulate a scan of a phantom and reconstruct it slice by slice.

        Returns when the reconstruction finishes or is cancelled. Callers
        that want to keep interacting with the agent should run this as a
        background task (see ``main``).
        """
        if self._running:
            raise RuntimeError('A reconstruction is already running')
        self._running = True
        self._cancel.clear()
        assert self.log_path is not None
        self.log_path.write_text(f'{time.ctime()} PREPARING\n')
        try:
            # The heavy numerical work is blocking, so run it in a thread.
            # This keeps the agent's event loop free to answer
            # get_progress / get_utilization / cancel while it runs.
            return await self.agent_run_sync(
                self._reconstruct_blocking, n_slices, size, n_angles,
            )
        finally:
            self._running = False

    @action
    async def get_progress(self) -> dict[str, Any]:
        """Report reconstruction progress by parsing the reconstruction log."""
        if self.log_path is None or not self.log_path.exists():
            return {'state': 'idle', 'done': 0, 'total': 0, 'percent': 0.0}
        lines = self.log_path.read_text().splitlines()
        state, done, total = 'idle', 0, 0
        for line in lines:
            if m := re.search(r'slice (\d+)/(\d+)', line):
                done, total = int(m[1]), int(m[2])
                state = 'running'
            for word in ('PREPARING', 'STARTED', 'FINISHED', 'CANCELLED',
                         'FAILED'):
                if word in line:
                    state = 'running' if word == 'STARTED' else word.lower()
                    if word == 'STARTED':
                        total = int(re.search(r'n_slices=(\d+)', line)[1])
                        done = 0
        pct = 100.0 * done / total if total else 0.0
        return {
            'state': state,
            'done': done,
            'total': total,
            'percent': round(pct, 1),
            'last_line': lines[-1] if lines else '',
        }

    @action
    async def get_utilization(self) -> dict[str, Any]:
        """Snapshot of CPU, memory and load on the agent's machine."""
        import psutil

        vm = psutil.virtual_memory()
        return {
            'hostname': socket.gethostname(),
            'cpu_percent': psutil.cpu_percent(interval=0.5),
            'load_avg': [round(x, 2) for x in os.getloadavg()],
            'mem_used_gb': round(vm.used / 1e9, 2),
            'mem_total_gb': round(vm.total / 1e9, 2),
            'mem_percent': vm.percent,
            'process_rss_gb': round(psutil.Process().memory_info().rss / 1e9, 2),
        }

    @action
    async def cancel(self) -> bool:
        """Ask a running reconstruction to stop after the current slice."""
        if not self._running:
            return False
        self._cancel.set()
        return True

    # -- the actual science -------------------------------------------------
    def _reconstruct_blocking(
        self, n_slices: int, size: int, n_angles: int,
    ) -> dict[str, Any]:
        """Filtered back-projection of a synthetic 3D phantom.

        Swap the body of the loop for tomopy / your own code; the parts that
        matter for the demo are writing progress to the log and checking
        ``self._cancel`` between units of work.
        """
        import numpy as np
        from skimage.data import shepp_logan_phantom
        from skimage.transform import iradon
        from skimage.transform import radon
        from skimage.transform import resize

        assert self.log_path is not None
        theta = np.linspace(0.0, 180.0, n_angles, endpoint=False)
        base = resize(shepp_logan_phantom(), (size, size))
        recon = np.zeros((n_slices, size, size), dtype=np.float32)
        t0 = time.time()

        with self.log_path.open('a', buffering=1) as log:
            log.write(f'{time.ctime()} STARTED n_slices={n_slices} '
                      f'size={size} n_angles={n_angles}\n')
            try:
                for i in range(n_slices):
                    if self._cancel.is_set():
                        log.write(f'{time.ctime()} CANCELLED at slice '
                                  f'{i}/{n_slices}\n')
                        return {'state': 'cancelled', 'slices_done': i,
                                'elapsed_s': round(time.time() - t0, 1)}
                    # Vary the phantom along z so slices differ.
                    z = 1.0 - abs(2 * i / max(n_slices - 1, 1) - 1) * 0.5
                    slice_ = base * z
                    sinogram = radon(slice_, theta=theta)          # "measure"
                    recon[i] = iradon(sinogram, theta=theta,
                                      filter_name='ramp')          # reconstruct
                    log.write(f'{time.ctime()} slice {i + 1}/{n_slices} '
                              f'done\n')
            except Exception as e:
                log.write(f'{time.ctime()} FAILED {e!r}\n')
                raise

            out = self.log_path.with_suffix('.npy')
            np.save(out, recon)
            elapsed = round(time.time() - t0, 1)
            log.write(f'{time.ctime()} FINISHED in {elapsed}s -> {out}\n')

        return {'state': 'finished', 'slices_done': n_slices,
                'elapsed_s': elapsed, 'output': str(out),
                'hostname': socket.gethostname()}


# --------------------------------------------------------------------------- #
# Driver: what you run on your laptop.
# --------------------------------------------------------------------------- #
def make_manager_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.local:
        from academy.exchange.local import LocalExchangeFactory

        # Everything in this process; no auth, no remote resources.
        return {'factory': LocalExchangeFactory(), 'executors': None}

    from academy.exchange.cloud.client import HttpExchangeFactory
    from globus_compute_sdk import Executor as GCExecutor

    gce = GCExecutor(
        endpoint_id=args.endpoint,
        user_endpoint_config=(
            json.loads(args.user_endpoint_config)
            if args.user_endpoint_config else None
        ),
    )
    return {
        # Hosted exchange; first run opens a Globus login.
        'factory': HttpExchangeFactory(auth_method='globus'),
        'executors': {'globus_compute': gce},
    }


async def main(args: argparse.Namespace) -> None:
    margs = make_manager_args(args)
    async with await Manager.from_exchange_factory(**margs) as manager:
        # Notifier lives in this process (the Manager's own event loop).
        notifier = await manager.launch(Notifier, executor='event_loop')

        # TomoAgent goes to Globus Compute (or stays local with --local).
        tomo = await manager.launch(
            TomoAgent,
            args=(notifier, args.workdir),
            executor=None if args.local else 'globus_compute',
        )
        print('Submitted TomoAgent, waiting for it to start '
              '(on a batch-scheduled MEP this can take a while)...')
        info = await notifier.wait_started(timeout=args.start_timeout)
        print(f'Agent is running on {await tomo.get_hostname()}')

        # Start the reconstruction in the background, so we can keep talking
        # to the agent while it runs.
        recon = asyncio.create_task(tomo.reconstruct(
            n_slices=args.slices, size=args.size, n_angles=args.angles,
        ))
        t0 = time.time()

        while not recon.done():
            progress, util = await asyncio.gather(
                tomo.get_progress(), tomo.get_utilization(),
            )
            print(f'[{time.time() - t0:6.1f}s] '
                  f'{progress["state"]:>9} {progress["done"]:>4}/'
                  f'{progress["total"]:<4} ({progress["percent"]:5.1f}%) | '
                  f'cpu {util["cpu_percent"]:5.1f}%  '
                  f'load {util["load_avg"][0]:5.2f}  '
                  f'mem {util["mem_percent"]:4.1f}%  '
                  f'@ {util["hostname"]}')

            if args.cancel_after and time.time() - t0 > args.cancel_after:
                print('Cancelling reconstruction...')
                await tomo.cancel()
                args.cancel_after = None

            await asyncio.wait({recon}, timeout=args.poll)

        result = await recon
        await notifier.notify('reconstruction-done', result)
        print(f'Log on remote machine: {info["log"]}')
        # Leaving the Manager context shuts the remote agent down.


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument('--endpoint', help='Globus Compute endpoint UUID')
    where.add_argument('--local', action='store_true',
                       help='run everything in this process (for testing)')
    p.add_argument('--user-endpoint-config', default=None,
                   help='JSON for a multi-user endpoint template '
                        '(account, queue, walltime, worker_init, ...)')
    p.add_argument('--workdir', default=None,
                   help='directory on the remote machine for log + output '
                        '(default: its temp dir)')
    p.add_argument('--slices', type=int, default=128)
    p.add_argument('--size', type=int, default=512, help='slice width (px)')
    p.add_argument('--angles', type=int, default=360)
    p.add_argument('--poll', type=float, default=3.0,
                   help='seconds between progress/utilization checks')
    p.add_argument('--cancel-after', type=float, default=None,
                   help='cancel the reconstruction after this many seconds')
    p.add_argument('--start-timeout', type=float, default=1800)
    return p.parse_args()


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main(parse_args()))
