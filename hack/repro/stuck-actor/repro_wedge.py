"""Reproduce an actor stuck in SUSPENDING or DELETING with its worker allocated.

Creates an actor from a template, breaks it one of three ways, then tears it down
the way a client would (SuspendActor, then DeleteActor) and samples the actor's
state and every worker's allocated.actors for a minute.

  oomhog  allocate past the worker's memory limit; the ateom container is
          OOM-killed and restarted, taking the sandbox with it
  killed  cancel SuspendActor a few hundred milliseconds in, mid-teardown, then
          tear down again
  seed    make the sandbox agent exit, then tear down (control case)

Usage: repro_wedge.py <oomhog|killed|seed> --template NAME
Exit code 0 means no worker was left allocated.
"""

import argparse
import secrets
import sys
import time

import grpc

from ateclient import ATESPACE, TEMPLATE_ATESPACE, Client, pb


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def census(c: Client, label: str):
    pinned = c.allocated()
    log(f"census[{label}] allocated={len(pinned)} {pinned} actors={c.actor_states()}")
    return pinned


def create_and_run(c: Client, template: str) -> str:
    name = "stuck-" + secrets.token_hex(4)
    c.call(
        "CreateActor",
        pb.CreateActorRequest(
            actor=pb.Actor(
                metadata=pb.ResourceMetadata(atespace=ATESPACE, name=name),
                actor_template=pb.ObjectRef(atespace=TEMPLATE_ATESPACE, name=template),
            )
        ),
    )
    for _ in range(24):
        try:
            c.call("ResumeActor", pb.ResumeActorRequest(actor=c.ref(name)), timeout=600)
            break
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.RESOURCE_EXHAUSTED:
                raise
            log("no free worker, retrying resume")
            time.sleep(5)
    else:
        raise SystemExit("no free worker")
    if not c.wait_ready(name):
        raise SystemExit(f"{name} never became ready")
    return name


def close(c: Client, name: str):
    try:
        c.call("SuspendActor", pb.SuspendActorRequest(actor=c.ref(name)), timeout=600)
        log("SuspendActor ok")
    except grpc.RpcError as e:
        log(f"SuspendActor: {e.code().name}: {e.details()[:200]}")
    try:
        c.call("DeleteActor", pb.DeleteActorRequest(actor=c.ref(name)), timeout=600)
        log("DeleteActor ok")
    except grpc.RpcError as e:
        log(f"DeleteActor: {e.code().name}: {e.details()[:200]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["oomhog", "killed", "seed"])
    ap.add_argument("--template", required=True, help="ActorTemplate name in TEMPLATE_ATESPACE")
    args = ap.parse_args()

    c = Client()
    census(c, "before")

    log(f"create ({args.mode}) template={args.template}")
    name = create_and_run(c, args.template)
    r = c.exec(name, "echo alive; id -u")
    log(f"{name} exec -> exit={r.get('exit_code')} out={(r.get('stdout') or '').strip()!r} err_type={r.get('error_type')}")

    if args.mode == "oomhog":
        r = c.exec(name, "nohup python3 -c \"a=bytearray(3*1024**3); import time; time.sleep(600)\" >/dev/null 2>&1 &", timeout=30)
        log(f"hog issued exit={r.get('exit_code')}; waiting 25s for the OOM")
        time.sleep(25)
    elif args.mode == "seed":
        r = c.exec(name, "nohup sh -c 'sleep 1; kill -TERM 1; sleep 3; kill -KILL 1; kill -9 -1' >/dev/null 2>&1 &", timeout=30)
        log(f"agent kill issued exit={r.get('exit_code')}; waiting 8s for the sandbox to exit")
        time.sleep(8)
    elif args.mode == "killed":
        fut = c.call_future("SuspendActor", pb.SuspendActorRequest(actor=c.ref(name)))
        time.sleep(0.3)
        fut.cancel()
        log("SuspendActor cancelled after 0.3s (simulated client death)")
        time.sleep(5)

    r = c.exec(name, "echo still-alive", timeout=20)
    log(f"post exec -> exit={r.get('exit_code')} err_type={r.get('error_type')} stderr={(r.get('stderr') or '')[:120]!r}")
    log(f"actor state before close: {c.state(name)}")
    close(c, name)

    pinned = None
    st = c.state(name)
    for i in range(6):
        st = c.state(name)
        pinned = census(c, f"after+{i * 10}s")
        log(f"actor {name}: {st}")
        if st == "GONE" and not pinned:
            break
        time.sleep(10)

    verdict = "NO PIN" if (st == "GONE" and not pinned) else f"PINNED (actor={st}, workers_allocated={pinned})"
    log(f"VERDICT[{args.mode}]: {verdict}")
    return 0 if verdict == "NO PIN" else 1


if __name__ == "__main__":
    sys.exit(main())
