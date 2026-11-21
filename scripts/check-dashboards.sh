#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DASHBOARDS_DIR="${REPO_ROOT}/infra/observability/grafana/dashboards"
OPTIONAL_FILE="${SCRIPT_DIR}/dashboards-optional.txt"
ENV_FILE="${REPO_ROOT}/infra/.env"
PROM_URL="${PROM_URL:-http://127.0.0.1:9090}"
GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"

python3 - "$DASHBOARDS_DIR" "$OPTIONAL_FILE" "$ENV_FILE" "$PROM_URL" "$GRAFANA_URL" << 'PYEOF'
import base64, glob, json, os, re, sys, urllib.error, urllib.parse, urllib.request

dashboards_dir, optional_path, env_path, prom_url, grafana_url = sys.argv[1:6]
prom_url = prom_url.rstrip("/")
grafana_url = grafana_url.rstrip("/")

# Load Grafana credentials from infra/.env
grafana_user = "admin"
grafana_password = "admin"
if os.path.isfile(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k == "GRAFANA_ADMIN_USER":
                grafana_user = v
            elif k == "GRAFANA_ADMIN_PASSWORD":
                grafana_password = v

# Load optional metrics allowlist
optional_allowlist = {}
if os.path.isfile(optional_path):
    with open(optional_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "|" in line:
                metric, just = [p.strip() for p in line.split("|", 1)]
                optional_allowlist[metric] = just
            else:
                optional_allowlist[line] = "Allowed optional metric"

PROMQL_KEYWORDS = {
    "sum", "rate", "histogram_quantile", "clamp_min", "increase", "count", "vector", "deriv",
    "avg", "min", "max", "by", "without", "on", "ignoring", "group_left", "group_right",
    "or", "and", "unless", "bool", "le", "round", "topk", "bottomk", "quantile", "stddev",
    "stdvar", "abs", "ceil", "floor", "exp", "sqrt", "ln", "log2", "log10", "time", "timestamp",
    "hour", "minute", "day_of_week", "day_of_month", "days_in_month", "year", "month",
    "changes", "resets", "idelta", "irate", "delta", "predict_linear", "label_replace",
    "label_join", "histogram_fraction", "sort", "sort_desc"
}

LABEL_NAMES = {
    "service", "method", "uri", "status", "outcome", "reason", "trigger", "datname",
    "state", "topic", "consumergroup", "consumer_group", "event_type", "failure_code",
    "final_status", "alertname", "severity", "alertstate", "job", "instance", "script",
    "result", "area", "operation", "route", "status_class"
}

def extract_metric_candidates(expr):
    """Extract candidate metric/rule names from PromQL expression."""
    tokens = set(re.findall(r'[a-zA-Z_:][a-zA-Z0-9_:]*', expr))
    candidates = tokens - PROMQL_KEYWORDS - LABEL_NAMES
    # filter out pure numbers or single characters
    return {c for c in candidates if len(c) > 1 and not c.isdigit()}

def query_prometheus(expr):
    # Substitute Grafana variables:
    # 1. Range variables -> 5m
    clean_expr = re.sub(r'\[\s*\$(?:__)?(?:rate_interval|interval|range)\s*\]', '[5m]', expr)
    clean_expr = re.sub(r'\$(?:__)?(?:rate_interval|interval|range)', '5m', clean_expr)
    clean_expr = re.sub(r'\$\{(?:__)?(?:rate_interval|interval|range)\}', '5m', clean_expr)

    # 2. Template variables -> .*
    # E.g. $service -> .*
    clean_expr = re.sub(r'\$([a-zA-Z0-9_]+)', '.*', clean_expr)
    clean_expr = re.sub(r'\$\{([a-zA-Z0-9_]+)\}', '.*', clean_expr)

    url = f"{prom_url}/api/v1/query?query={urllib.parse.quote(clean_expr)}"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") != "success":
                return False, clean_expr, f"PromQL error: {data}", []
            results = data.get("data", {}).get("result", [])
            return True, clean_expr, None, results
    except Exception as e:
        return False, clean_expr, str(e), []

def check_grafana_provisioned(uid):
    url = f"{grafana_url}/api/dashboards/uid/{uid}"
    req = urllib.request.Request(url)
    auth_header = base64.b64encode(f"{grafana_user}:{grafana_password}".encode()).decode()
    req.add_header("Authorization", f"Basic {auth_header}")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            meta = data.get("meta", {})
            is_prov = meta.get("isProvisioned") is True or meta.get("provisioned") is True
            return True, is_prov, meta
    except Exception as e:
        return False, False, str(e)

dashboard_files = sorted(glob.glob(os.path.join(dashboards_dir, "*.json")))
if not dashboard_files:
    print(f"[FAIL] No dashboard JSON files found in {dashboards_dir}")
    sys.exit(1)

print(f"=== Running Automated Dashboard Validation & Checks ===")
print(f"Dashboards directory: {dashboards_dir} ({len(dashboard_files)} files)")
print(f"Prometheus URL:       {prom_url}")
print(f"Grafana URL:          {grafana_url}")
print(f"Optional Allowlist:   {optional_path} ({len(optional_allowlist)} entries)\n")

overall_failed = False
summary_rows = []

for filepath in dashboard_files:
    filename = os.path.basename(filepath)
    with open(filepath) as f:
        try:
            dash = json.load(f)
        except Exception as e:
            print(f"[FAIL] {filename}: Invalid JSON: {e}")
            overall_failed = True
            continue

    # (a) Validate JSON structure, unique panel IDs, uid, title, datasource uid
    uid = dash.get("uid")
    title = dash.get("title")
    if not uid or not title:
        print(f"[FAIL] {filename}: Missing 'uid' or 'title' in dashboard root")
        overall_failed = True
        continue

    panels = dash.get("panels", [])
    panel_ids = set()
    dup_ids = set()
    bad_datasource = []

    for p in panels:
        pid = p.get("id")
        if pid in panel_ids:
            dup_ids.add(pid)
        panel_ids.add(pid)

        # Check datasource uid
        ds = p.get("datasource", {})
        if isinstance(ds, dict) and ds.get("uid") != "prometheus":
            bad_datasource.append((p.get("title"), ds))

        for t in p.get("targets", []):
            tds = t.get("datasource", {})
            if isinstance(tds, dict) and tds.get("uid") != "prometheus":
                bad_datasource.append((p.get("title"), tds))

    if dup_ids:
        print(f"[FAIL] {filename}: Duplicate panel IDs: {sorted(dup_ids)}")
        overall_failed = True
    if bad_datasource:
        print(f"[FAIL] {filename}: Incorrect datasource uid: {bad_datasource}")
        overall_failed = True

    # (b) Validate PromQL expressions against Prometheus
    targets_count = 0
    with_data_count = 0
    optional_empty_count = 0
    failed_targets_count = 0

    for p in panels:
        p_title = p.get("title", f"Panel {p.get('id')}")
        for t in p.get("targets", []):
            expr = t.get("expr", "").strip()
            if not expr:
                continue
            targets_count += 1
            success, clean_expr, err, results = query_prometheus(expr)

            if not success:
                print(f"[FAIL] {filename} | {p_title} | Error executing: {clean_expr}\n       Reason: {err}")
                failed_targets_count += 1
                overall_failed = True
            elif len(results) > 0:
                with_data_count += 1
            else:
                # Result is empty -> check allowlist
                candidates = extract_metric_candidates(expr)
                matched_allowlist = [c for c in candidates if c in optional_allowlist]
                if matched_allowlist:
                    optional_empty_count += 1
                else:
                    print(f"[FAIL] {filename} | {p_title} | NO DATA for query: {clean_expr}")
                    print(f"       Extracted candidates: {sorted(candidates)}")
                    print(f"       Not found in {optional_path}!")
                    failed_targets_count += 1
                    overall_failed = True

    # (c) Check Grafana provisioning via API
    graf_ok, is_provisioned, meta = check_grafana_provisioned(uid)
    if not graf_ok:
        print(f"[FAIL] {filename}: Failed to query Grafana API for uid={uid}: {meta}")
        overall_failed = True
    elif not is_provisioned:
        print(f"[FAIL] {filename}: Grafana reports dashboard uid={uid} is NOT provisioned (meta: {meta})")
        overall_failed = True

    dash_status = "PASS" if (failed_targets_count == 0 and graf_ok and is_provisioned and not dup_ids and not bad_datasource) else "FAIL"
    summary_rows.append({
        "file": filename,
        "uid": uid,
        "title": title,
        "panels": len(panels),
        "targets": targets_count,
        "with_data": with_data_count,
        "optional_empty": optional_empty_count,
        "failed": failed_targets_count,
        "provisioned": is_provisioned,
        "status": dash_status
    })

# Print summary table
print("\n" + "=" * 105)
print(f"{'Dashboard Title':<28} | {'UID':<20} | {'Panels':<6} | {'Targets':<7} | {'Data':<5} | {'OptEmpty':<8} | {'Prov':<5} | {'Status':<6}")
print("-" * 105)
for r in summary_rows:
    prov_str = "YES" if r["provisioned"] else "NO"
    print(f"{r['title']:<28} | {r['uid']:<20} | {r['panels']:<6} | {r['targets']:<7} | {r['with_data']:<5} | {r['optional_empty']:<8} | {prov_str:<5} | {r['status']:<6}")
print("=" * 105)

if overall_failed:
    print("\n[FAIL] One or more dashboard checks failed.")
    sys.exit(1)
else:
    print("\n[SUCCESS] All dashboards validated, executed, and provisioned cleanly.")
PYEOF
