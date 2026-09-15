"""One-line summary of workers, their allocation, and actor states.

Usage: census.py

Run before a repro to confirm every worker shows allocated.actors = 0 on an idle
pool, and after one to see which worker a stuck actor is still holding.
"""

import time

from ateclient import Client


def main():
    c = Client()
    workers = c.workers()
    allocated = [(w.worker_pod[-6:], w.status.allocated.actors) for w in workers if w.status.HasField("allocated") and w.status.allocated.actors > 0]
    print(
        f"{time.strftime('%H:%M:%S')} workers={len(workers)} free={len(workers) - len(allocated)} "
        f"allocated={len(allocated)} {allocated} actors={c.actor_states()}"
    )
    for w in workers:
        alloc = w.status.allocated.actors if w.status.HasField("allocated") else 0
        print(f"    pod=..{w.worker_pod[-6:]} node=..{w.node_name[-5:]} alloc.actors={alloc}")


if __name__ == "__main__":
    main()
