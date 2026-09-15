"""Try every lifecycle verb against a stuck actor and report what each returns.

Usage: recover_probe.py <actor-name>

Against an actor left in SUSPENDING or DELETING, this shows that no API call
gets it out: SuspendActor and ResumeActor fail, plain DeleteActor is rejected,
and DeleteActor(any_state=true) flips it to DELETING and leaves the worker
allocated.
"""

import sys
import time

import grpc

from ateclient import Client, pb


def call(c: Client, name: str, request) -> str:
    try:
        resp = c.call(name, request, timeout=600)
        return f"OK {type(resp).__name__}"
    except grpc.RpcError as e:
        return f"{e.code().name}: {e.details()[:220]}"


def main():
    actor = sys.argv[1]
    c = Client()
    ref = c.ref(actor)
    print("state:", c.state(actor), "| workers allocated:", len(c.allocated()))
    print("SuspendActor again          ->", call(c, "SuspendActor", pb.SuspendActorRequest(actor=ref)))
    time.sleep(3)
    print("state:", c.state(actor))
    print("ResumeActor                 ->", call(c, "ResumeActor", pb.ResumeActorRequest(actor=ref)))
    time.sleep(3)
    print("state:", c.state(actor))
    print("DeleteActor                 ->", call(c, "DeleteActor", pb.DeleteActorRequest(actor=ref)))
    print("DeleteActor(any_state=true) ->", call(c, "DeleteActor", pb.DeleteActorRequest(actor=ref, any_state=True)))
    for i in range(4):
        time.sleep(5)
        print(f"+{(i + 1) * 5}s state: {c.state(actor)} | workers allocated: {len(c.allocated())}")


if __name__ == "__main__":
    main()
