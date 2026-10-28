#!/usr/bin/env python3
"""
Collect BDR (Replication Manager) run history from the Cloudera Manager REST API
and write data.json for the dashboard (index.html).

Run it from cron, for example every 15 minutes:
    */15 * * * * CM_PASS='...' /usr/bin/python3 /opt/bdr-dashboard/collect_bdr.py \
        --host cm-host.example.com --user bdr_report --out /var/www/bdr/data.json \
        --extra-csv /var/log/bdr_jobs/extra_runs.csv --jobs-file /opt/bdr-dashboard/jobs.csv

Needs: Python 3.6+ and `pip install requests`.
Use a read-only Cloudera Manager account.

Optional files
  --extra-csv   Runs of jobs that are not BDR policies (the 19 Sqoop jobs, the 5 cron jobs).
                Columns: job,category,start,end,status,bytes,message
                Written for you by job_wrap.sh.
  --jobs-file   Names of every extra job that should appear even on days it did not run.
                Columns: name,category   (category: Sqoop or Cron)
"""
import argparse
import csv
import datetime as dt
import json
import os
import sys

import requests

# Policy names, compared in lower case. Everything else that is a Hive
# replication is counted as "Databases", and any other HDFS policy as "HDFS".
REALTIME = {"nta_real_time_data_sync", "session_logins_active_2109"}
FACT = {"bsedb_impala_trade_fact_1809", "bsedb_impala_order_fact_1809", "fact_egrdb_bcxdb_impala_1809"}
HDFS = {"iccl_output_files_hdfs", "incoming_hdfs", "staging_hdfs", "sebicotf_hdfs",
        "amit_kudu_test", "backupdata_hdfs", "backupdata_1_bkp_hdfs"}


def categorize(name, is_hive):
    n = name.lower()
    if n in REALTIME:
        return "Real-time tables"
    if n in FACT:
        return "Fact tables"
    if n in HDFS:
        return "HDFS"
    return "Databases" if is_hive else "HDFS"


def parse_time(s):
    if not s:
        return None
    return dt.datetime.strptime(s.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z") \
        if "." in s else dt.datetime.strptime(s.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")


def extract_result(cmd):
    """Bytes and files copied, whether the policy is HDFS or Hive."""
    hdfs = cmd.get("hdfsResult") or {}
    hive = cmd.get("hiveResult") or {}
    data = hive.get("dataReplicationResult") or {}
    src = hdfs if hdfs else data
    return int(src.get("numBytesCopied") or 0), int(src.get("numFilesCopied") or 0)


def to_run(cmd, name, category):
    if cmd.get("active"):
        status = "RUNNING"
    elif cmd.get("success"):
        status = "SUCCESS"
    else:
        status = "FAILED"
    nbytes, nfiles = extract_result(cmd)
    return {
        "job": name,
        "category": category,
        "start": cmd.get("startTime"),
        "end": cmd.get("endTime"),
        "status": status,
        "bytes": nbytes,
        "files": nfiles,
        "message": (cmd.get("resultMessage") or "")[:300],
        "_id": cmd.get("id"),
    }


class CM:
    def __init__(self, args):
        scheme = "https" if args.tls else "http"
        port = args.port or (7183 if args.tls else 7180)
        self.root = "%s://%s:%s/api" % (scheme, args.host, port)
        self.s = requests.Session()
        self.s.auth = (args.user, args.password)
        self.s.verify = not args.insecure
        self.debug = args.debug
        self.ver = args.api_version or self.s.get(self.root + "/version", timeout=30).text.strip()

    def get(self, path, **params):
        r = self.s.get("%s/%s%s" % (self.root, self.ver, path), params=params, timeout=120)
        r.raise_for_status()
        return r.json()


def history_for(cm, base, sched, cutoff):
    """Page through a schedule's history until runs are older than the cutoff."""
    out, seen, offset, page = [], set(), 0, 100
    while True:
        try:
            chunk = cm.get("%s/%s/history" % (base, sched["id"]), limit=page, offset=offset, view="full")
        except requests.HTTPError:
            chunk = {"items": sched.get("history", [])}
            offset = None
        items = chunk.get("items", chunk if isinstance(chunk, list) else [])
        if not items:
            break
        for c in items:
            if c.get("id") not in seen:
                seen.add(c.get("id"))
                out.append(c)
        oldest = min((parse_time(c.get("startTime")) for c in items if c.get("startTime")), default=None)
        if offset is None or len(items) < page or (oldest and oldest < cutoff):
            break
        offset += page
    return out


def collect_cm(cm, days, only_cluster, debug):
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    expected, runs = [], []
    for cl in cm.get("/clusters").get("items", []):
        if only_cluster and cl["name"] != only_cluster:
            continue
        for svc in cm.get("/clusters/%s/services" % cl["name"]).get("items", []):
            if svc.get("type") not in ("HDFS", "HIVE"):
                continue
            base = "/clusters/%s/services/%s/replications" % (cl["name"], svc["name"])
            try:
                schedules = cm.get(base, limit=1000, view="summary").get("items", [])
            except requests.HTTPError as e:
                print("skip %s/%s: %s" % (cl["name"], svc["name"], e), file=sys.stderr)
                continue
            for sc in schedules:
                is_hive = "hiveArguments" in sc
                name = sc.get("displayName") or sc.get("name") or "schedule_%s" % sc["id"]
                cat = categorize(name, is_hive)
                if sc.get("paused"):
                    pass  # paused policies still appear; they will show as "Not run"
                expected.append({"job": name, "category": cat})
                hist = history_for(cm, base, sc, cutoff)
                if debug and hist:
                    print("--- raw history entry for %s ---" % name)
                    print(json.dumps(hist[0], indent=2)[:3000])
                for c in hist:
                    t = parse_time(c.get("startTime"))
                    if t and t >= cutoff:
                        runs.append(to_run(c, name, cat))
    return expected, runs


def read_extra(path):
    runs = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if not row.get("job") or not row.get("start"):
                continue
            runs.append({
                "job": row["job"].strip(),
                "category": (row.get("category") or "Cron").strip(),
                "start": row["start"].strip(),
                "end": (row.get("end") or "").strip() or None,
                "status": (row.get("status") or "SUCCESS").strip().upper(),
                "bytes": int(float(row.get("bytes") or 0)),
                "message": (row.get("message") or "").strip(),
            })
    return runs


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True, help="Cloudera Manager host")
    ap.add_argument("--port", type=int, help="default 7183 with --tls, else 7180")
    ap.add_argument("--tls", action="store_true", help="use https")
    ap.add_argument("--insecure", action="store_true", help="do not verify the TLS certificate")
    ap.add_argument("--user", default=os.environ.get("CM_USER"), help="or set CM_USER")
    ap.add_argument("--password", default=os.environ.get("CM_PASS"), help="or set CM_PASS")
    ap.add_argument("--api-version", help="for example v41; detected when omitted")
    ap.add_argument("--cluster", help="only this cluster; default is all")
    ap.add_argument("--days", type=int, default=30, help="how many days of history to keep (default 30)")
    ap.add_argument("--out", default="data.json")
    ap.add_argument("--extra-csv", help="runs of Sqoop and cron jobs")
    ap.add_argument("--jobs-file", help="expected Sqoop and cron jobs: name,category")
    ap.add_argument("--timezone", default="Asia/Kolkata", help="time zone the dashboard displays")
    ap.add_argument("--debug", action="store_true", help="print one raw history entry per policy")
    args = ap.parse_args()
    if not args.user or not args.password:
        ap.error("set --user/--password or CM_USER/CM_PASS")

    cm = CM(args)
    expected, runs = collect_cm(cm, args.days, args.cluster, args.debug)

    if args.jobs_file and os.path.exists(args.jobs_file):
        with open(args.jobs_file, newline="") as f:
            for row in csv.reader(f):
                if len(row) >= 2 and row[0].strip() and row[0].strip().lower() != "name":
                    expected.append({"job": row[0].strip(), "category": row[1].strip()})
    if args.extra_csv and os.path.exists(args.extra_csv):
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
        for r in read_extra(args.extra_csv):
            try:
                if parse_time(r["start"]) < cutoff:
                    continue
            except ValueError:
                pass
            runs.append(r)

    for r in runs:
        r.pop("_id", None)
    uniq = {}
    for e in expected:
        uniq[e["job"]] = e
    out = {
        "generated": dt.datetime.now(dt.timezone.utc).isoformat(),
        "timezone": args.timezone,
        "expected": list(uniq.values()),
        "runs": runs,
    }
    tmp = args.out + ".tmp"
    with open(tmp, "w") as f:
        json.dump(out, f)
    os.replace(tmp, args.out)  # the dashboard never sees a half-written file
    print("wrote %s: %d jobs, %d runs" % (args.out, len(out["expected"]), len(runs)))


if __name__ == "__main__":
    main()
