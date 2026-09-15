"""Minimal ateapi and router client for the stuck-actor repro scripts.

Talks to ateapi over gRPC with the repository's generated Python stubs
(benchmarking/locust/common) and to actors through the atenet router over HTTP.

Settings come from the environment, defaulting to the in-cluster service names:
  ATEAPI_ADDR          ateapi gRPC address           (api.ate-system.svc:443)
  ATENET_ADDR          atenet router HTTP address    (atenet-router.ate-system.svc:80)
  ATESPACE             atespace the actors live in   (default)
  TEMPLATE_ATESPACE    atespace of the ActorTemplate (defaults to ATESPACE)
  ATEAPI_TOKEN_FILE    bearer token for ateapi; if unset one is minted for the
                       ATEAPI_SA ServiceAccount with the kubernetes client
  ATEAPI_CA_FILE       CA bundle for ateapi's TLS certificate; if unset it is
                       read from the ClusterTrustBundle for ATEAPI_CA_SIGNER
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import grpc

sys.path.insert(0, os.environ.get("ATEAPI_STUBS_DIR", str(Path(__file__).resolve().parents[3] / "benchmarking" / "locust")))
from common import ateapi_pb2 as pb  # noqa: E402
from common import ateapi_pb2_grpc as rpc  # noqa: E402

ATEAPI_ADDR = os.environ.get("ATEAPI_ADDR", "api.ate-system.svc:443")
ATENET_ADDR = os.environ.get("ATENET_ADDR", "atenet-router.ate-system.svc:80")
ATESPACE = os.environ.get("ATESPACE", "default")
TEMPLATE_ATESPACE = os.environ.get("TEMPLATE_ATESPACE", ATESPACE)
SERVER_NAME = os.environ.get("ATEAPI_SERVER_NAME", "api.ate-system.svc")
SA = os.environ.get("ATEAPI_SA", "ate-client")
SA_NAMESPACE = os.environ.get("ATEAPI_SA_NAMESPACE", "ate-system")
CA_SIGNER = os.environ.get("ATEAPI_CA_SIGNER", "servicedns.podcert.ate.dev/identity")

STATE = {v.number: v.name.replace("ACTOR_STATE_", "") for v in pb.ActorState.DESCRIPTOR.values}


def _kube():
    from kubernetes import client, config

    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client


def _token() -> str:
    path = os.environ.get("ATEAPI_TOKEN_FILE")
    if path:
        return Path(path).read_text().strip()
    client = _kube()
    req = client.AuthenticationV1TokenRequest(
        spec=client.V1TokenRequestSpec(audiences=[SERVER_NAME], expiration_seconds=3600)
    )
    return client.CoreV1Api().create_namespaced_service_account_token(SA, SA_NAMESPACE, req).status.token


def _ca() -> bytes:
    path = os.environ.get("ATEAPI_CA_FILE")
    if path:
        return Path(path).read_bytes()
    client = _kube()
    for api_name in ("CertificatesV1beta1Api", "CertificatesV1alpha1Api"):
        api = getattr(client, api_name, None)
        if api is None:
            continue
        try:
            items = api().list_cluster_trust_bundle(field_selector=f"spec.signerName={CA_SIGNER}").items
        except Exception:
            continue
        pem = "".join(i.spec.trust_bundle for i in items)
        if pem:
            return pem.encode()
    raise RuntimeError(f"no ClusterTrustBundle for signer {CA_SIGNER!r}; set ATEAPI_CA_FILE")


class Client:
    def __init__(self):
        creds = grpc.ssl_channel_credentials(root_certificates=_ca())
        self._channel = grpc.secure_channel(ATEAPI_ADDR, creds, options=[("grpc.ssl_target_name_override", SERVER_NAME)])
        self._stub = rpc.ControlStub(self._channel)
        self._metadata = (("authorization", "Bearer " + _token()),)

    def call(self, name: str, request, timeout: float = 120):
        return getattr(self._stub, name)(request, metadata=self._metadata, timeout=timeout)

    def call_future(self, name: str, request):
        return getattr(self._stub, name).future(request, metadata=self._metadata)

    def ref(self, name: str):
        return pb.ObjectRef(atespace=ATESPACE, name=name)

    def state(self, name: str) -> str:
        try:
            actor = self.call("GetActor", pb.GetActorRequest(actor=self.ref(name)))
            return STATE.get(actor.status.state, str(actor.status.state))
        except grpc.RpcError as e:
            return "GONE" if e.code() == grpc.StatusCode.NOT_FOUND else f"ERR:{e.code().name}"

    def workers(self):
        out, token = [], ""
        while True:
            resp = self.call("ListWorkers", pb.ListWorkersRequest(page_size=200, page_token=token))
            out.extend(resp.workers)
            token = resp.next_page_token
            if not token:
                return out

    def allocated(self):
        """(pod suffix, actors) for every worker with an actor allocated."""
        return [
            (w.worker_pod[-6:], w.status.allocated.actors)
            for w in self.workers()
            if w.status.HasField("allocated") and w.status.allocated.actors > 0
        ]

    def actor_states(self):
        resp = self.call("ListActors", pb.ListActorsRequest(atespace=ATESPACE, page_size=500))
        counts = {}
        for a in resp.actors:
            k = STATE.get(a.status.state, str(a.status.state))
            counts[k] = counts.get(k, 0) + 1
        return counts

    def _http(self, method: str, name: str, path: str, body=None, timeout: float = 30):
        req = urllib.request.Request(
            f"http://{ATENET_ADDR}{path}",
            method=method,
            headers={"ate-target-actor": f"{ATESPACE}/{name}", "content-type": "application/json"},
            data=None if body is None else json.dumps(body).encode(),
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()

    def wait_ready(self, name: str, timeout: float = 600) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                status, _ = self._http("GET", name, "/readyz", timeout=10)
                if status == 200:
                    return True
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(2)
        return False

    def exec(self, name: str, command: str, timeout: float = 120) -> dict:
        try:
            _, data = self._http("POST", name, "/exec", {"command": command, "timeout_s": float(timeout)}, timeout=timeout + 30)
        except urllib.error.HTTPError as e:
            return {"exit_code": None, "stderr": f"router returned {e.code}: {e.read()[:200]!r}", "error_type": "sandbox"}
        except (urllib.error.URLError, TimeoutError) as e:
            return {"exit_code": None, "stderr": str(e), "error_type": "sandbox"}
        return json.loads(data or b"{}")
