# Reproduce an actor stuck in SUSPENDING / DELETING with its worker allocated

Three scripts that drive ateapi and the atenet router directly, using the
repository's generated Python stubs (`benchmarking/locust/common`). Run them
from a checkout, or copy `benchmarking/locust` next to them and point
`ATEAPI_STUBS_DIR` at it.

## Requirements

- Python 3 with `grpcio` and `protobuf`; `kubernetes` as well unless you supply
  a token and CA by hand (below).
- Network access to ateapi and the atenet router. The defaults are the
  in-cluster service names, so the simplest place to run is a pod in the
  cluster.
- An ActorTemplate in `TEMPLATE_ATESPACE` whose image has `python3` (the OOM
  trigger uses it).

## Configuration

Everything is an environment variable; the defaults suit an in-cluster run:

| Variable | Default | Meaning |
|---|---|---|
| `ATEAPI_ADDR` | `api.ate-system.svc:443` | ateapi gRPC address |
| `ATENET_ADDR` | `atenet-router.ate-system.svc:80` | router HTTP address |
| `ATESPACE` | `default` | atespace the actors are created in |
| `TEMPLATE_ATESPACE` | `ATESPACE` | atespace of the ActorTemplate |
| `ATEAPI_TOKEN_FILE` | unset | bearer token for ateapi; if unset, one is minted for `ATEAPI_SA` (`ate-client` in `ate-system`) with audience `api.ate-system.svc` |
| `ATEAPI_CA_FILE` | unset | CA for ateapi's certificate; if unset, read from the ClusterTrustBundle for `servicedns.podcert.ate.dev/identity` |

To supply the credentials by hand instead:

```
kubectl -n ate-system create token ate-client --audience=api.ate-system.svc --duration=1h > token
kubectl get clustertrustbundle -o jsonpath='{range .items[?(@.spec.signerName=="servicedns.podcert.ate.dev/identity")]}{.spec.trustBundle}{end}' > ca.pem
export ATEAPI_TOKEN_FILE=$PWD/token ATEAPI_CA_FILE=$PWD/ca.pem
```

## Scripts

- `repro_wedge.py <mode> --template NAME`: creates an actor, runs a command in
  it, breaks it, then tears it down the way a client would (`SuspendActor`,
  then `DeleteActor`) and samples the actor's state and every worker's
  `allocated.actors` for a minute. Exit code 0 means no worker was left
  allocated.
  - `oomhog`: allocate past the worker's memory limit. The ateom container is
    OOM-killed and restarted, taking the sandbox with it. `SuspendActor` fails
    with `runsc checkpoint: exit status 128` and the actor is left
    `SUSPENDING` with its worker allocated.
  - `killed`: cancel `SuspendActor` a few hundred milliseconds in, mid-teardown,
    then tear down again.
  - `seed`: make the sandbox agent exit, then tear down. Control case.
- `recover_probe.py <actor>`: against a stuck actor, issues `SuspendActor`,
  `ResumeActor`, `DeleteActor` and `DeleteActor(any_state)` in turn and prints
  each result and the worker allocation afterwards.
- `census.py`: one-line summary of workers, allocation, and actor states. Run
  it first to confirm `allocated = 0` on an idle pool.

## Typical run

```
python3 repro_wedge.py oomhog --template <template>
# -> VERDICT[oomhog]: PINNED (actor=SUSPENDING, workers_allocated=[...])
python3 recover_probe.py <actor from the line above>
# -> DeleteActor(any_state=true) -> INTERNAL ... runsc delete: exit status 128
#    +20s state: DELETING | workers allocated: 1
```

Today the allocated worker is only released by deleting its pod.
