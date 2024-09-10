from flask import Flask, Response, abort, jsonify, make_response, request, redirect
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Enum,
    Gauge,
    Info,
    Summary,
    multiprocess,
)
from prometheus_client.openmetrics.exposition import generate_latest
from kubernetes import client, config
from concurrent.futures import ThreadPoolExecutor, wait

import socket
import os
import json
from time import sleep

registry = CollectorRegistry()
multiprocess.MultiProcessCollector(registry)

CEPHFS_CLIENT_FLUSHES = Gauge(
    "cephfs_client_flushes",
    "cephfs_client_flushes",
    ["address", "clientid", "identifier", "volume", "namespace", "pod"],
)
CEPHFS_CLIENT_COMPLETED = Gauge(
    "cephfs_client_completed",
    "cephfs_client_completed",
    ["address", "clientid", "identifier", "volume", "namespace", "pod"],
)
CEPHFS_CLIENT_INFLIGHT = Gauge(
    "cephfs_client_inflight",
    "cephfs_client_inflight",
    ["address", "clientid", "identifier", "volume", "namespace", "pod"],
)


class OCObject(object):
    def __init__(self, obj):
        for o in obj:
            setattr(self, o, obj[o])

    def to_dict(self):
        return dict(tuple(map(lambda x: (x, getattr(self, x)), self.__dict__.keys())))


def collect_ocp_infos():
    config.load_kube_config(config_file=os.environ.get("KUBECONFIG"))
    v1 = client.CoreV1Api()
    pods = v1.list_pod_for_all_namespaces().to_dict()["items"]
    # pvcs = v1.list_persistent_volume_claim_for_all_namespaces().to_dict()["items"]
    pvs = v1.list_persistent_volume().to_dict()["items"]

    def pvfilter(pvs, dfilter="cephfs"):
        # get on object returns None as value so {} doesn't work
        for p in pvs:
            if p.get("spec") != None:
                if p.get("spec").get("csi") != None:
                    if dfilter in str(p.get("spec").get("csi").get("driver")):
                        yield p

    def pvcfilter(pvs, pods):
        # get on object returns None as value so {} doesn't work
        for p in pvs:
            claim = p.get("spec").get("claim_ref")
            if any([claim == None, p.get("spec").get("csi") == None]):
                continue
            for pod in filter(
                lambda x: all(
                    [
                        claim.get("namespace") == x.get("metadata").get("namespace"),
                        x.get("spec").get("volumes") != None,
                    ]
                ),
                pods,
            ):
                if claim.get("name") in map(
                    lambda y: y.get("persistent_volume_claim").get("claim_name"),
                    filter(
                        lambda z: z.get("persistent_volume_claim") != None,
                        pod.get("spec").get("volumes", []),
                    ),
                ):
                    yield OCObject(
                        dict(
                            subvolumepath=p.get("spec")
                            .get("csi")
                            .get("volume_attributes")
                            .get("subvolumePath"),
                            namespace=pod.get("metadata").get("namespace"),
                            name=pod.get("metadata").get("name"),
                            node=pod.get("status").get("host_ip"),
                        )
                    )

    cephfspvs = pvfilter(pvs, dfilter="cephfs")
    return pvcfilter(cephfspvs, pods)


class ClientStats(object):
    def __init__(self, data):
        self.__parse_socket__(data)

    def __parse_socket__(self, data):
        for k in data:
            setattr(self, k, data[k])


class Stats(object):
    def __init__(self, data):
        self.clients = []
        self.__parse_socket__(data)

    def __parse_socket__(self, data):
        data = json.loads(data[4:])
        for entry in data:
            self.clients.append(ClientStats(entry))

    def __split_inst__(self, inst):
        clientid, address = inst.split()
        clientid = clientid.split(".")[-1]
        address = address.split(":")
        identifier = address[-1].split("/")[-1]
        address = address[1]
        return clientid, identifier, address

    def report(self):
        try:
            ocinfo = list(map(lambda x: OCObject(x), json.load(open("/tmp/ocpinfo"))))
        except Exception as ocerr:
            app.logger.error(f"couldn't load ocpinfo from json {ocerr}")
            ocinfo = []
        for c in self.clients:
            clientid, identifier, address = self.__split_inst__(c.inst)
            for oc in list(
                filter(
                    lambda x: all(
                        [
                            x.subvolumepath == c.client_metadata["root"],
                            x.node == address,
                        ]
                    ),
                    ocinfo,
                )
            ):
                flushes = CEPHFS_CLIENT_FLUSHES.labels(
                    address,
                    clientid,
                    identifier,
                    c.client_metadata["root"],
                    oc.namespace,
                    oc.name,
                )._value.get()
                if all([c.num_completed_flushes == 0, flushes > 0]):
                    c.num_completed_flushes = flushes
                elif c.num_completed_flushes == flushes:
                    c.num_completed_flushes = flushes
                CEPHFS_CLIENT_FLUSHES.labels(
                    address,
                    clientid,
                    identifier,
                    c.client_metadata["root"],
                    oc.namespace,
                    oc.name,
                ).set(c.num_completed_flushes)
                completed = CEPHFS_CLIENT_COMPLETED.labels(
                    address,
                    clientid,
                    identifier,
                    c.client_metadata["root"],
                    oc.namespace,
                    oc.name,
                )._value.get()
                if all([c.num_completed_requests == 0, completed > 0]):
                    c.num_completed_requests = completed
                elif completed == c.num_completed_requests:
                    c.num_completed_requests = completed
                CEPHFS_CLIENT_COMPLETED.labels(
                    address,
                    clientid,
                    identifier,
                    c.client_metadata["root"],
                    oc.namespace,
                    oc.name,
                ).set(c.num_completed_requests)
                CEPHFS_CLIENT_INFLIGHT.labels(
                    address,
                    clientid,
                    identifier,
                    c.client_metadata["root"],
                    oc.namespace,
                    oc.name,
                ).set(c.requests_in_flight)


def get_stats(asok):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect((asok))
    cmd = json.dumps({"prefix": "client ls"}).replace('"', '"')
    s.send(cmd.encode("utf8") + b"\0")
    data = b""
    while True:
        d = s.recv(1024)
        if d == b"":
            break
        data += d
    s.close()
    return data


def get_info():
    while True:
        ocpinfo = list(collect_ocp_infos())
        if len(ocpinfo) < 1000:
            DEF_SLEEP = 60
        elif len(ocpinfo) < 5000:
            DEF_SLEEP = 120
        else:
            DEF_SLEEP = 300
        with open("/tmp/ocpinfo", "w") as ocw:
            ocw.write(json.dumps(list(map(lambda x: x.to_dict(), ocpinfo))))
        sleep(int(os.environ.get("API_INTERVAL", DEF_SLEEP)))


def get_reports(asok):
    Stats(get_stats(asok)).report()


app = Flask(__name__)


@app.route("/", methods=["GET"])
def index():
    return redirect("/metrics", code=302)


@app.route("/metrics", methods=["GET"])
def generate_metrics():
    if "," in os.environ.get("ASOK"):
        asoks = os.environ.get("ASOK").split(",")
    else:
        asoks = [os.environ.get("ASOK")]
    threads = []
    with ThreadPoolExecutor(max_workers=len(asoks)) as tpe:
        for sok in asoks:
            threads.append(tpe.submit(get_reports, sok))
        wait(threads)
    return Response(
        response=generate_latest(registry),
        status=200,
    )


@app.route("/health", methods=["GET"])
@app.route("/liveness", methods=["GET"])
@app.route("/startup", methods=["GET"])
def health():
    return jsonify(dict(state="OK")), 200


tpe = ThreadPoolExecutor(max_workers=1)
octhread = tpe.submit(get_info)

if __name__ == "__main__":
    app.debug = False
    app.run(
        host=os.environ.get("LISTEN", "0.0.0.0"),
        port=int(os.environ.get("PORT", 8080)),
        threaded=True,
    )
