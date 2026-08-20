from flask import Flask, render_template, request, jsonify, send_file
import io
import json
import math
import random
import re
import sqlite3
import time
import zipfile
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.parse import urlparse

import numpy as np
import pandas as pd

app = Flask(__name__)

STATE = {
    "df": None,
    "clean_df": None,
    "valid_df": None,
    "invalid_df": None,
    "quarantine_df": None,
    "issues": [],
    "temp_valid_df": None,
    "temp_invalid_df": None,
    "temp_issues": [],
    "rules": [],
    "runs": [],
    "source_name": None,
    "sources": {},
    "active_source_id": None,
    "integration": {"project_name": None, "source_ids": [], "base_source_id": None, "steps": [], "combined_df": None, "report": {}, "normalized": {}},
    "ingestion_ms": 0.0,
    "validation_ms": 0.0,
    "last_action_ms": 0.0,
    "last_rows_per_sec": 0.0,
    "partition_count": 4,
    "partition_key": None,
    "partition_stats": [],
    "kafka_connected": False,
    "spark_connected": False,
    "stream_running": False,
    "stream_processed": 0,
    "stream_valid": 0,
    "stream_invalid": 0,
    "stream_quarantined": 0,
    "stream_events": [],
    "stream_quality_history": [],
    "kafka_offsets": [0, 0, 0, 0],
    "kafka_lag": [0, 0, 0, 0],
    "stream_rate": 0.0,
    "chat_history": [],
    "clean_plan": [],
    "clean_preview_df": None,
    "clean_preview_changes": [],
    "clean_quarantine_df": None,
}

KB = [
    {
        "title": "Completeness",
        "keywords": {"completeness", "missing", "null", "empty", "required"},
        "text": "Completeness measures how much required data is present. Improve it by identifying mandatory fields, preventing nulls at ingestion, and using approved imputation only where business rules allow it.",
    },
    {
        "title": "Validity",
        "keywords": {"validity", "invalid", "range", "datatype", "format", "regex", "allowed"},
        "text": "Validity measures whether values follow defined formats, ranges, datatypes and allowed-value rules. Improve it with schema rules, range checks and controlled value lists.",
    },
    {
        "title": "Uniqueness",
        "keywords": {"uniqueness", "duplicate", "duplicates", "unique", "deduplicate"},
        "text": "Uniqueness checks whether records that should represent one entity are repeated. Use business keys or composite keys such as name + date of birth + location rather than relying on a name alone.",
    },
    {
        "title": "Consistency",
        "keywords": {"consistency", "consistent", "status", "standard", "mapping"},
        "text": "Consistency measures whether the same type of information follows the same representation across records. Standardize categories, date formats, units and codes before downstream use.",
    },
    {
        "title": "Integrity",
        "keywords": {"integrity", "foreign", "relationship", "key", "reference", "id"},
        "text": "Integrity measures whether identifiers and relationships are trustworthy. Primary keys should be present and unique, and foreign-key values should exist in their referenced datasets.",
    },
    {
        "title": "Spark partitions",
        "keywords": {"spark", "partition", "partitions", "shuffle", "worker"},
        "text": "Spark partitions divide work so tasks can run in parallel. Operations such as groupBy and joins may shuffle records by key when records across partitions need to be compared.",
    },
    {
        "title": "Kafka streaming",
        "keywords": {"kafka", "stream", "streaming", "lag", "offset", "topic"},
        "text": "Kafka carries events through topics and partitions. Consumer lag is the difference between produced offsets and processed offsets; growing lag can indicate the processing layer is falling behind.",
    },
]



def _slug_source_name(name):
    base = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name or "dataset")).strip("_") or "dataset"
    candidate = base
    n = 2
    while candidate in STATE.get("sources", {}):
        candidate = f"{base}_{n}"
        n += 1
    return candidate


def _reset_dataset_state():
    STATE["clean_df"] = None
    STATE["valid_df"] = None
    STATE["invalid_df"] = None
    STATE["quarantine_df"] = None
    STATE["issues"] = []
    STATE["temp_valid_df"] = None
    STATE["temp_invalid_df"] = None
    STATE["temp_issues"] = []
    STATE["clean_plan"] = []
    STATE["clean_preview_df"] = None
    STATE["clean_preview_changes"] = []
    STATE["clean_quarantine_df"] = None


def _register_source(df, name, source_type, details=None, select=True):
    source_id = _slug_source_name(name)
    record = {
        "id": source_id,
        "name": str(name),
        "type": source_type,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "filename": details.get("filename") if details else None,
        "details": details or {},
        "df": df.copy(),
        "created_at": now_text(),
    }
    STATE.setdefault("sources", {})[source_id] = record
    if select:
        _select_source(source_id)
    return record


def _select_source(source_id):
    record = STATE.get("sources", {}).get(source_id)
    if record is None:
        return False
    STATE["active_source_id"] = source_id
    STATE["df"] = record["df"].copy()
    STATE["source_name"] = record["name"]
    _reset_dataset_state()
    STATE["partition_key"] = None
    return True


def _sync_active_source_from_df():
    sid = STATE.get("active_source_id")
    if sid and sid in STATE.get("sources", {}) and STATE.get("df") is not None:
        STATE["sources"][sid]["df"] = STATE["df"].copy()
        STATE["sources"][sid]["rows"] = int(len(STATE["df"]))
        STATE["sources"][sid]["columns"] = int(len(STATE["df"].columns))


def _source_summaries():
    return [{k: v for k, v in src.items() if k != "df"} | {"active": src["id"] == STATE.get("active_source_id")} for src in STATE.get("sources", {}).values()]


def _read_sql_dump(file):
    """Load a SQL dump into an in-memory SQLite database and return every table.

    This supports common SQL dump files containing CREATE TABLE and INSERT
    statements. Each table becomes an independent DataGuard dataset.
    """
    raw = file.read()
    if not raw:
        raise ValueError("The SQL file is empty")
    try:
        script = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        script = raw.decode("latin-1")

    conn = sqlite3.connect(":memory:")
    try:
        conn.executescript(script)
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            conn,
        )["name"].tolist()
        if not tables:
            raise ValueError("SQL file contains no CREATE TABLE definitions")

        datasets = []
        for table in tables:
            safe_table = table.replace('"', '""')
            df = pd.read_sql(f'SELECT * FROM "{safe_table}"', conn)
            datasets.append((table, df))
        return datasets
    except sqlite3.Error as exc:
        raise ValueError(
            "Could not read SQL dump. Use a SQL dump containing standard CREATE TABLE and INSERT statements. "
            f"SQLite reported: {exc}"
        ) from exc
    finally:
        conn.close()


def _read_uploaded_source(file):
    name = (file.filename or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(file), "CSV"
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(file), "Excel"
    if name.endswith(".json"):
        return pd.read_json(file), "JSON"
    if name.endswith(".parquet"):
        return pd.read_parquet(file), "Parquet"
    if name.endswith(".sql"):
        return _read_sql_dump(file), "SQL Dump"
    if name.endswith(".db") or name.endswith(".sqlite") or name.endswith(".sqlite3"):
        raw = file.read()
        temp_path = f"/tmp/dataguard_{time.time_ns()}.db"
        with open(temp_path, "wb") as fh:
            fh.write(raw)
        conn2 = sqlite3.connect(temp_path)
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", conn2)["name"].tolist()
        if not tables:
            conn2.close()
            raise ValueError("SQLite database contains no tables")
        df = pd.read_sql(f'SELECT * FROM "{tables[0].replace(chr(34), chr(34)*2)}"', conn2)
        conn2.close()
        return df, "SQLite DB"
    raise ValueError("Supported sources: CSV, Excel, JSON, Parquet, SQL Dump and SQLite DB")


def _read_rest_json(url, headers=None):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("REST URL must start with http:// or https://")
    req = Request(url, headers=headers or {"User-Agent": "DataGuard/1.0"})
    with urlopen(req, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        for key in ("data", "results", "items", "records"):
            if isinstance(payload.get(key), list):
                return pd.json_normalize(payload[key])
        return pd.json_normalize([payload])
    raise ValueError("REST response must be a JSON object or array")


def _integration_state():
    return STATE.setdefault("integration", {"project_name": None, "source_ids": [], "base_source_id": None, "steps": [], "combined_df": None, "report": {}, "normalized": {}})


def _dtype_group(series):
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "text"


def _join_key_candidates(left_df, right_df):
    if left_df is None or right_df is None:
        return []
    out = []
    right_lookup = {str(c).lower(): c for c in right_df.columns}
    for lc in left_df.columns:
        key = str(lc).lower()
        if key in right_lookup:
            rc = right_lookup[key]
            out.append({"left": lc, "right": rc, "score": 1.0, "reason": "Same column name"})
    # Also suggest normalized names such as customerID/customer_id/customerId.
    def norm(x):
        return re.sub(r"[^a-z0-9]", "", str(x).lower())
    right_norm = {norm(c): c for c in right_df.columns}
    for lc in left_df.columns:
        rc = right_norm.get(norm(lc))
        if rc and not any(x["left"] == lc and x["right"] == rc for x in out):
            out.append({"left": lc, "right": rc, "score": 0.92, "reason": "Normalized column-name match"})
    # ID/key candidates by overlap.
    for lc in left_df.columns:
        for rc in right_df.columns:
            if any(x["left"] == lc and x["right"] == rc for x in out):
                continue
            ltxt, rtxt = str(lc).lower(), str(rc).lower()
            if ("id" in ltxt or "key" in ltxt) and ("id" in rtxt or "key" in rtxt):
                if _dtype_group(left_df[lc]) == _dtype_group(right_df[rc]):
                    lvals = set(left_df[lc].dropna().astype(str).head(5000))
                    rvals = set(right_df[rc].dropna().astype(str).head(5000))
                    if lvals and rvals:
                        overlap = len(lvals & rvals) / max(1, len(lvals))
                        if overlap >= 0.15:
                            out.append({"left": lc, "right": rc, "score": round(min(0.9, overlap), 3), "reason": f"Key overlap {overlap:.0%}"})
    out.sort(key=lambda x: (-x["score"], str(x["left"])))
    return out[:12]


def _integration_source_record(source_id):
    return STATE.get("sources", {}).get(source_id)


def _integration_report(df, steps, project_name):
    report = {
        "project_name": project_name or "Integrated Project",
        "sources": len(steps) + (1 if steps else 0),
        "final_rows": int(len(df)) if df is not None else 0,
        "final_columns": int(len(df.columns)) if df is not None else 0,
        "joins": [],
        "issues": [],
    }
    for step in steps:
        report["joins"].append({
            "left": step.get("left_name"), "right": step.get("right_name"),
            "left_key": step.get("left_key"), "right_key": step.get("right_key"),
            "how": step.get("how"), "left_rows": step.get("left_rows"),
            "right_rows": step.get("right_rows"), "matched": step.get("matched"),
            "unmatched": step.get("unmatched"), "match_rate": step.get("match_rate"),
            "type_conversion": step.get("type_conversion", False),
        })
        if step.get("unmatched", 0):
            report["issues"].append({
                "type": "REFERENTIAL_INTEGRITY",
                "severity": "MEDIUM",
                "rows": int(step.get("unmatched", 0)),
                "reason": f"{step.get('unmatched', 0)} left-side row(s) had no matching {step.get('right_name')} key",
            })
    return report


def _build_integration_preview(state):
    if not state.get("base_source_id"):
        raise ValueError("Choose a base dataset first")
    base = _integration_source_record(state["base_source_id"])
    if not base:
        raise ValueError("Base dataset no longer exists")
    current = base["df"].copy()
    steps_out = []
    used_names = set(map(str, current.columns))
    for step in state.get("steps", []):
        right = _integration_source_record(step["right_source_id"])
        if not right:
            raise ValueError(f"Source not found: {step.get('right_source_id')}")
        left_key, right_key = step["left_key"], step["right_key"]
        if left_key not in current.columns:
            raise ValueError(f"Join column '{left_key}' is not available after the previous joins")
        rdf = right["df"].copy()
        if right_key not in rdf.columns:
            raise ValueError(f"Join column '{right_key}' is not available in {right['name']}")
        conversion = False
        ltype, rtype = _dtype_group(current[left_key]), _dtype_group(rdf[right_key])
        if ltype != rtype:
            if step.get("normalize", True):
                current[left_key] = current[left_key].astype("string")
                rdf[right_key] = rdf[right_key].astype("string")
                conversion = True
            else:
                raise ValueError(f"Type mismatch: {left_key} is {ltype}, {right_key} is {rtype}. Enable normalization.")
        left_before = len(current)
        right_rows = len(rdf)
        right_key_series = rdf[right_key]
        matched_keys = set(right_key_series.dropna().astype(str))
        left_key_series = current[left_key]
        matched_mask = left_key_series.notna() & left_key_series.astype(str).isin(matched_keys)
        matched = int(matched_mask.sum())
        unmatched = int(left_before - matched)
        how = step.get("how", "left")
        suffix = step.get("suffix", "right")
        # Avoid collisions by suffixing right-side duplicate names.
        overlap = [c for c in rdf.columns if c in current.columns and c != right_key]
        if overlap:
            rdf = rdf.rename(columns={c: f"{c}_{suffix}" for c in overlap})
        current = current.merge(rdf, left_on=left_key, right_on=right_key, how=how, suffixes=("", f"_{suffix}"))
        # If right key name differs, retain a single left key in the combined result.
        if right_key != left_key and right_key in current.columns:
            current = current.drop(columns=[right_key])
        step_copy = dict(step)
        step_copy.update({"left_rows": left_before, "right_rows": right_rows, "matched": matched, "unmatched": unmatched,
                          "match_rate": round((matched / left_before * 100) if left_before else 100.0, 2),
                          "type_conversion": conversion,
                          "left_name": base["name"] if not steps_out else steps_out[-1]["right_name"],
                          "right_name": right["name"],
                          "result_rows": len(current)})
        steps_out.append(step_copy)
    return current, steps_out

def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def json_safe(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if math.isnan(value):
            return None
        return float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if pd.isna(value):
        return None
    return value


def dataframe_records(df, limit=100):
    if df is None:
        return []
    safe = df.head(limit).copy()
    for col in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[col]):
            safe[col] = safe[col].astype(str)
    rows = safe.to_dict(orient="records")
    return [{k: json_safe(v) for k, v in row.items()} for row in rows]


def make_demo_orders(rows=650):
    rng = np.random.default_rng(42)
    countries = ["India", "Ireland", "UK", "Germany", "France"]
    districts = ["Tirunelveli", "Chennai", "Dublin", "Galway", "Berlin"]
    villages = ["Eruvadi", "Panagudi", "Salthill", "Oranmore", "Mitte"]

    df = pd.DataFrame({
        "orderID": np.arange(10001, 10001 + rows),
        "customerID": [f"C{n:04d}" for n in rng.integers(1, 280, rows)],
        "customerName": rng.choice(["John", "Maria", "Aisha", "Liam", "Arun", "Priya", "Noah"], rows),
        "country": rng.choice(countries, rows),
        "district": rng.choice(districts, rows),
        "village": rng.choice(villages, rows),
        "productID": rng.integers(1, 80, rows),
        "quantity": rng.integers(1, 20, rows),
        "unitPrice": np.round(rng.uniform(5, 500, rows), 2),
        "orderDate": pd.date_range("2026-08-01", periods=rows, freq="15min"),
        "status": rng.choice(["NEW", "PAID", "SHIPPED", "CANCELLED"], rows),
    })

    # Intentional quality problems for the demo.
    df.loc[3, "customerID"] = None
    df.loc[8, "unitPrice"] = -25
    df.loc[12, "quantity"] = 0
    df.loc[18, "status"] = "UNKNOWN"
    df.loc[19, "orderID"] = df.loc[18, "orderID"]
    df.loc[40, ["customerName", "country", "district", "village"]] = ["John", "India", "Tirunelveli", "Eruvadi"]
    df.loc[41, ["customerName", "country", "district", "village"]] = ["John", "India", "Tirunelveli", "Eruvadi"]
    df.loc[42, ["customerName", "country", "district", "village"]] = ["John", "India", "Tirunelveli", "Panagudi"]
    return df


def get_active_df(prefer_clean=True):
    if prefer_clean and STATE["clean_df"] is not None:
        return STATE["clean_df"]
    return STATE["df"]


def default_validation(df):
    issues = []
    masks = []
    if df is None or df.empty:
        return issues, pd.Series(dtype=bool)

    for col in df.columns:
        null_mask = df[col].isna() | (df[col].astype(str).str.strip().eq("") if df[col].dtype == "object" else False)
        if bool(null_mask.any()):
            issues.append({
                "rule_id": "DEFAULT_NULL",
                "issue_type": "NULL",
                "columns": [col],
                "column": col,
                "severity": "HIGH",
                "rows": int(null_mask.sum()),
                "reason": f"{int(null_mask.sum())} missing value(s) found in {col}",
            })
            masks.append(null_mask)

    full_dup = df.duplicated(keep=False)
    if bool(full_dup.any()):
        issues.append({
            "rule_id": "DEFAULT_DUPLICATE_ROW",
            "issue_type": "DUPLICATE_ROW",
            "columns": list(df.columns),
            "column": "ALL",
            "severity": "MEDIUM",
            "rows": int(full_dup.sum()),
            "reason": f"{int(full_dup.sum())} fully duplicated row(s) found",
        })
        masks.append(full_dup)

    for col in df.select_dtypes(include=np.number).columns:
        if col.lower().endswith("id"):
            continue
        neg = df[col] < 0
        if bool(neg.any()):
            issues.append({
                "rule_id": "DEFAULT_NEGATIVE",
                "issue_type": "NEGATIVE_VALUE",
                "columns": [col],
                "column": col,
                "severity": "HIGH",
                "rows": int(neg.sum()),
                "reason": f"{int(neg.sum())} negative value(s) found in {col}",
            })
            masks.append(neg)

    if "orderID" in df.columns:
        dup = df["orderID"].duplicated(keep=False) & df["orderID"].notna()
        if bool(dup.any()):
            issues.append({
                "rule_id": "DEFAULT_ORDER_KEY",
                "issue_type": "DUPLICATE_KEY",
                "columns": ["orderID"],
                "column": "orderID",
                "severity": "HIGH",
                "rows": int(dup.sum()),
                "reason": "orderID should be unique",
            })
            masks.append(dup)

    if "quantity" in df.columns:
        q = pd.to_numeric(df["quantity"], errors="coerce")
        bad = q <= 0
        if bool(bad.fillna(False).any()):
            issues.append({
                "rule_id": "DEFAULT_QUANTITY",
                "issue_type": "BUSINESS_RULE",
                "columns": ["quantity"],
                "column": "quantity",
                "severity": "HIGH",
                "rows": int(bad.fillna(False).sum()),
                "reason": "quantity must be greater than 0",
            })
            masks.append(bad.fillna(False))

    if "status" in df.columns:
        allowed = {"NEW", "PAID", "SHIPPED", "CANCELLED"}
        bad = ~df["status"].isin(allowed) & df["status"].notna()
        if bool(bad.any()):
            issues.append({
                "rule_id": "DEFAULT_STATUS",
                "issue_type": "ALLOWED_VALUES",
                "columns": ["status"],
                "column": "status",
                "severity": "MEDIUM",
                "rows": int(bad.sum()),
                "reason": "status must be NEW, PAID, SHIPPED or CANCELLED",
            })
            masks.append(bad)

    combined = pd.Series(False, index=df.index)
    for m in masks:
        combined |= m.reindex(df.index, fill_value=False)
    return issues, combined


def dtype_mask(series, expected):
    expected = expected.lower()
    non_null = series.notna()
    if expected == "numeric":
        parsed = pd.to_numeric(series, errors="coerce")
        return non_null & parsed.isna()
    if expected == "integer":
        parsed = pd.to_numeric(series, errors="coerce")
        return non_null & (parsed.isna() | ((parsed % 1) != 0))
    if expected == "date":
        parsed = pd.to_datetime(series, errors="coerce")
        return non_null & parsed.isna()
    if expected == "boolean":
        valid = {"true", "false", "1", "0", "yes", "no"}
        return non_null & ~series.astype(str).str.lower().isin(valid)
    if expected == "string":
        return pd.Series(False, index=series.index)
    return pd.Series(False, index=series.index)


def evaluate_custom_rules(df, rules=None):
    rules = STATE["rules"] if rules is None else rules
    issues = []
    combined = pd.Series(False, index=df.index) if df is not None else pd.Series(dtype=bool)
    if df is None or df.empty:
        return issues, combined

    for rule in rules:
        if not rule.get("enabled", True):
            continue
        rule_type = rule.get("type")
        columns = [c for c in rule.get("columns", []) if c in df.columns]
        params = rule.get("params", {})
        severity = rule.get("severity", "MEDIUM")
        if not columns:
            continue

        mask = pd.Series(False, index=df.index)
        reason = ""

        if rule_type == "duplicate":
            mask = df.duplicated(subset=columns, keep=False)
            reason = f"Duplicate combination detected using: {', '.join(columns)}"

        elif rule_type == "required":
            for col in columns:
                m = df[col].isna()
                if df[col].dtype == "object":
                    m |= df[col].astype(str).str.strip().eq("")
                mask |= m
            reason = f"Required value missing in: {', '.join(columns)}"

        elif rule_type == "unique":
            col = columns[0]
            mask = df[col].duplicated(keep=False) & df[col].notna()
            reason = f"{col} must be unique"

        elif rule_type == "range":
            col = columns[0]
            values = pd.to_numeric(df[col], errors="coerce")
            min_v = params.get("min")
            max_v = params.get("max")
            if min_v not in (None, ""):
                mask |= values < float(min_v)
            if max_v not in (None, ""):
                mask |= values > float(max_v)
            reason = f"{col} is outside configured range"

        elif rule_type == "allowed_values":
            col = columns[0]
            values = [str(v).strip() for v in params.get("values", []) if str(v).strip()]
            mask = ~df[col].astype(str).isin(values) & df[col].notna()
            reason = f"{col} contains values outside: {', '.join(values)}"

        elif rule_type == "datatype":
            col = columns[0]
            expected = params.get("datatype", "string")
            mask = dtype_mask(df[col], expected)
            reason = f"{col} must match datatype {expected}"

        elif rule_type == "regex":
            col = columns[0]
            pattern = params.get("pattern", "")
            try:
                mask = df[col].notna() & ~df[col].astype(str).str.match(pattern, na=False)
            except re.error:
                mask = pd.Series(False, index=df.index)
                reason = f"Invalid regex pattern: {pattern}"
            else:
                reason = f"{col} does not match regex {pattern}"

        elif rule_type == "non_negative":
            for col in columns:
                values = pd.to_numeric(df[col], errors="coerce")
                mask |= values < 0
            reason = f"Negative values are not allowed in: {', '.join(columns)}"

        if bool(mask.fillna(False).any()):
            issue = {
                "rule_id": rule.get("id"),
                "rule_name": rule.get("name", rule_type.replace("_", " ").title()),
                "issue_type": rule_type.upper(),
                "columns": columns,
                "column": ", ".join(columns),
                "severity": severity,
                "rows": int(mask.fillna(False).sum()),
                "reason": reason,
            }
            issues.append(issue)
            combined |= mask.fillna(False)

    return issues, combined


def split_by_mask(df, mask):
    if df is None:
        return None, None
    mask = mask.reindex(df.index, fill_value=False)
    invalid = df[mask].copy()
    valid = df[~mask].copy()
    return valid.reset_index(drop=True), invalid.reset_index(drop=True)


def add_quarantine_metadata(df, issues, label="Validation failure"):
    if df is None:
        return None
    q = df.copy()
    if not q.empty:
        q["_quarantine_timestamp"] = now_text()
        q["_reason"] = label
        q["_issue_groups"] = len(issues)
    return q


def auto_fix(df):
    if df is None:
        return None
    fixed = df.copy()
    for col in fixed.columns:
        if fixed[col].isna().any():
            if pd.api.types.is_numeric_dtype(fixed[col]):
                median = fixed[col].median()
                fixed[col] = fixed[col].fillna(0 if pd.isna(median) else median)
            else:
                mode = fixed[col].mode()
                fixed[col] = fixed[col].fillna(mode.iloc[0] if not mode.empty else "UNKNOWN")

    fixed = fixed.drop_duplicates().copy()
    if "orderID" in fixed.columns:
        fixed = fixed[~fixed["orderID"].duplicated(keep="first") | fixed["orderID"].isna()].copy()
    for col in fixed.select_dtypes(include=np.number).columns:
        if not col.lower().endswith("id"):
            fixed[col] = fixed[col].clip(lower=0)
    if "quantity" in fixed.columns:
        fixed.loc[pd.to_numeric(fixed["quantity"], errors="coerce") <= 0, "quantity"] = 1
    if "status" in fixed.columns:
        allowed = {"NEW", "PAID", "SHIPPED", "CANCELLED"}
        fixed.loc[~fixed["status"].isin(allowed), "status"] = "NEW"
    return fixed.reset_index(drop=True)


def calculate_dq_breakdown(df, issues=None, invalid_df=None):
    if df is None or df.empty:
        return {
            "overall": 0.0,
            "completeness": 0.0,
            "validity": 0.0,
            "uniqueness": 0.0,
            "consistency": 0.0,
            "integrity": 0.0,
            "tips": ["Load a dataset to calculate data-quality dimensions."],
        }

    rows, cols = df.shape
    total_cells = max(rows * cols, 1)
    missing = int(df.isna().sum().sum())
    for col in df.select_dtypes(include="object").columns:
        missing += int(df[col].astype(str).str.strip().eq("").sum())
    completeness = max(0.0, 100 * (1 - missing / total_cells))

    duplicated_rows = int(df.duplicated(keep=False).sum())
    # If the user created composite duplicate rules, use the strongest enabled
    # business-key duplicate signal as part of the uniqueness dimension.
    for rule in STATE.get("rules", []):
        if rule.get("enabled", True) and rule.get("type") == "duplicate":
            cols_for_dup = [c for c in rule.get("columns", []) if c in df.columns]
            if cols_for_dup:
                duplicated_rows = max(duplicated_rows, int(df.duplicated(subset=cols_for_dup, keep=False).sum()))
    uniqueness = max(0.0, 100 * (1 - duplicated_rows / max(rows, 1)))

    if invalid_df is None:
        _, default_mask = default_validation(df)
        invalid_rows = int(default_mask.sum())
    else:
        invalid_rows = len(invalid_df)
    validity = max(0.0, 100 * (1 - invalid_rows / max(rows, 1)))

    consistency_failures = 0
    consistency_checks = 0
    for col in df.columns:
        if df[col].dtype == "object":
            non_null = df[col].dropna().astype(str)
            if len(non_null):
                stripped = non_null.str.strip()
                consistency_checks += len(stripped)
                consistency_failures += int((stripped != non_null).sum())
    if "status" in df.columns:
        allowed = {"NEW", "PAID", "SHIPPED", "CANCELLED"}
        consistency_checks += int(df["status"].notna().sum())
        consistency_failures += int((~df["status"].isin(allowed) & df["status"].notna()).sum())
    consistency = 100.0 if consistency_checks == 0 else max(0.0, 100 * (1 - consistency_failures / consistency_checks))

    key_cols = [c for c in df.columns if c.lower().endswith("id")]
    key_checks = 0
    key_failures = 0
    for col in key_cols:
        key_checks += len(df)
        key_failures += int(df[col].isna().sum())
        if col.lower() in {"orderid", "id"}:
            key_failures += int((df[col].duplicated(keep=False) & df[col].notna()).sum())
    integrity = 100.0 if key_checks == 0 else max(0.0, 100 * (1 - key_failures / max(key_checks, 1)))

    metrics = {
        "completeness": round(completeness, 1),
        "validity": round(validity, 1),
        "uniqueness": round(uniqueness, 1),
        "consistency": round(consistency, 1),
        "integrity": round(integrity, 1),
    }
    metrics["overall"] = round(sum(metrics.values()) / 5, 1)

    tips = []
    if metrics["completeness"] < 95:
        worst = df.isna().sum().sort_values(ascending=False)
        col = worst.index[0] if len(worst) else "required fields"
        tips.append(f"Completeness: review missing values in '{col}' and mark truly mandatory columns as Required.")
    if metrics["validity"] < 95:
        tips.append("Validity: add Range, Datatype and Allowed Values rules to columns that currently accept inconsistent values.")
    if metrics["uniqueness"] < 95:
        tips.append("Uniqueness: define a business duplicate key using the exact columns that identify one entity, not a name alone.")
    if metrics["consistency"] < 95:
        tips.append("Consistency: standardize categories, whitespace, date formats and status codes before downstream processing.")
    if metrics["integrity"] < 95:
        tips.append("Integrity: ensure key columns are complete and unique; when multiple datasets are loaded, add foreign-key checks.")
    if not tips:
        tips.append("DQ is strong. Keep monitoring schema drift and streaming error rates so quality does not degrade over time.")
    metrics["tips"] = tips
    return metrics


def profile_dataframe(df):
    if df is None:
        return {"summary": {}, "columns": []}
    columns = []
    for col in df.columns:
        s = df[col]
        row = {
            "column": col,
            "dtype": str(s.dtype),
            "missing": int(s.isna().sum()),
            "missing_pct": round(float(s.isna().mean() * 100), 2),
            "unique": int(s.nunique(dropna=True)),
            "unique_pct": round(float(s.nunique(dropna=True) / max(len(s), 1) * 100), 2),
        }
        if pd.api.types.is_numeric_dtype(s):
            row.update({
                "min": json_safe(s.min()),
                "max": json_safe(s.max()),
                "mean": round(float(s.mean()), 3) if s.notna().any() else None,
            })
        else:
            top = s.dropna().astype(str).value_counts().head(3)
            row["top_values"] = ", ".join([f"{idx} ({cnt})" for idx, cnt in top.items()])
        columns.append(row)
    memory_mb = float(df.memory_usage(deep=True).sum() / (1024 * 1024))
    return {
        "summary": {
            "rows": len(df),
            "columns": len(df.columns),
            "memory_mb": round(memory_mb, 3),
            "numeric_columns": len(df.select_dtypes(include=np.number).columns),
            "text_columns": len(df.select_dtypes(include="object").columns),
            "missing_cells": int(df.isna().sum().sum()),
        },
        "columns": columns,
    }


def column_visualization(df, column):
    if df is None or column not in df.columns:
        return {"type": "bar", "labels": [], "values": [], "title": "No data"}
    s = df[column]
    if pd.api.types.is_numeric_dtype(s):
        clean = pd.to_numeric(s, errors="coerce").dropna()
        if clean.empty:
            return {"type": "bar", "labels": [], "values": [], "title": column}
        counts, bins = np.histogram(clean, bins=min(12, max(5, int(math.sqrt(len(clean))))))
        labels = [f"{bins[i]:.1f}–{bins[i+1]:.1f}" for i in range(len(counts))]
        return {"type": "bar", "labels": labels, "values": counts.tolist(), "title": f"Distribution of {column}"}
    top = s.fillna("<NULL>").astype(str).value_counts().head(12)
    return {"type": "bar", "labels": top.index.tolist(), "values": [int(x) for x in top.values], "title": f"Top values in {column}"}


def compute_partitions(df, count=4, key=None):
    if df is None or df.empty:
        return [], 0.0
    count = max(1, min(int(count), 32))
    partitions = []

    if key and key in df.columns:
        hashes = pd.util.hash_pandas_object(df[key].astype(str), index=False).astype("uint64")
        ids = (hashes % count).astype(int)
        groups = [(pid, df[ids == pid]) for pid in range(count)]
        mode = f"Hash repartition by {key}"
    else:
        idx_groups = np.array_split(np.arange(len(df)), count)
        groups = [(pid, df.iloc[idx]) for pid, idx in enumerate(idx_groups)]
        mode = "Sequential preview"

    counts = []
    for pid, part in groups:
        counts.append(len(part))
        sample = []
        if key and key in part.columns:
            sample = part[key].dropna().astype(str).head(4).tolist()
        partitions.append({
            "partition": pid,
            "rows": len(part),
            "share_pct": round(len(part) / len(df) * 100, 2),
            "memory_kb": round(float(part.memory_usage(deep=True).sum() / 1024), 2),
            "sample_keys": ", ".join(sample),
            "mode": mode,
        })
    avg = np.mean(counts) if counts else 0
    skew = 0.0 if avg == 0 else round(max(counts) / avg, 2)
    return partitions, skew


def update_performance(start, rows):
    elapsed_ms = (time.perf_counter() - start) * 1000
    STATE["last_action_ms"] = round(elapsed_ms, 2)
    STATE["last_rows_per_sec"] = round(rows / max(elapsed_ms / 1000, 0.000001), 1)
    return elapsed_ms


def add_run(status, df=None, issues=None, mode="default"):
    df = STATE["df"] if df is None else df
    issues = STATE["issues"] if issues is None else issues
    dq = calculate_dq_breakdown(df, issues)
    STATE["runs"].insert(0, {
        "run_id": "RUN-" + datetime.now().strftime("%H%M%S%f")[:10],
        "time": now_text(),
        "source": STATE["source_name"] or "Unknown",
        "mode": mode,
        "records": 0 if df is None else len(df),
        "issues": len(issues),
        "dq_score": dq["overall"],
        "duration_ms": STATE["last_action_ms"],
        "rows_per_sec": STATE["last_rows_per_sec"],
        "status": status,
    })
    STATE["runs"] = STATE["runs"][:100]


def generate_stream_event(force_error=False):
    event = {
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "orderID": random.randint(20000, 99999),
        "customerID": f"C{random.randint(1, 999):04d}",
        "productID": random.randint(1, 100),
        "quantity": random.randint(1, 12),
        "unitPrice": round(random.uniform(5, 500), 2),
        "status": random.choice(["NEW", "PAID", "SHIPPED", "CANCELLED"]),
    }

    if force_error or random.random() < 0.13:
        bad = random.choice(["NULL", "NEGATIVE", "STATUS", "QUANTITY"])
        if bad == "NULL":
            event["customerID"] = None
        elif bad == "NEGATIVE":
            event["unitPrice"] = -round(random.uniform(1, 100), 2)
        elif bad == "STATUS":
            event["status"] = "BAD_STATUS"
        else:
            event["quantity"] = 0

    event_df = pd.DataFrame([event])
    default_issues, default_mask = default_validation(event_df)
    custom_issues, custom_mask = evaluate_custom_rules(event_df)
    invalid = bool((default_mask | custom_mask).any())
    all_issues = default_issues + custom_issues

    STATE["stream_processed"] += 1
    partition = random.randrange(len(STATE["kafka_offsets"]))
    STATE["kafka_offsets"][partition] += 1
    if invalid:
        STATE["stream_invalid"] += 1
        STATE["stream_quarantined"] += 1
        event["result"] = "INVALID"
        event["issue"] = all_issues[0]["issue_type"] if all_issues else "RULE"
    else:
        STATE["stream_valid"] += 1
        event["result"] = "VALID"
        event["issue"] = ""

    event["kafkaPartition"] = partition
    event["offset"] = STATE["kafka_offsets"][partition]
    STATE["stream_events"].insert(0, event)
    STATE["stream_events"] = STATE["stream_events"][:120]

    total = max(STATE["stream_processed"], 1)
    score = round(STATE["stream_valid"] / total * 100, 1)
    STATE["stream_quality_history"].append({"time": event["timestamp"], "quality": score})
    STATE["stream_quality_history"] = STATE["stream_quality_history"][-80:]


def dataframe_to_sql_snapshot(df, issues, dq):
    conn = sqlite3.connect(":memory:")
    if df is not None:
        safe = df.copy()
        for col in safe.columns:
            if pd.api.types.is_datetime64_any_dtype(safe[col]):
                safe[col] = safe[col].astype(str)
        safe.to_sql("current_data", conn, index=False, if_exists="replace")
    issue_df = pd.DataFrame(issues or [], columns=["issue_type", "column", "severity", "rows", "reason"]) if not issues else pd.DataFrame(issues)
    for col in issue_df.columns:
        issue_df[col] = issue_df[col].apply(lambda v: json.dumps(v) if isinstance(v, (list, dict, tuple, set)) else v)
    issue_df.to_sql("validation_issues", conn, index=False, if_exists="replace")
    pd.DataFrame([{k: v for k, v in dq.items() if k != "tips"}]).to_sql("dq_metrics", conn, index=False, if_exists="replace")
    return conn


def find_column_in_question(question, columns):
    q = question.lower()
    exact = [c for c in columns if c.lower() in q]
    if exact:
        return exact[0]
    normalized = {re.sub(r"[^a-z0-9]", "", c.lower()): c for c in columns}
    qn = re.sub(r"[^a-z0-9 ]", "", q)
    for key, original in normalized.items():
        if key and key in qn.replace(" ", ""):
            return original
    return None


def retrieve_kb(question):
    words = set(re.findall(r"[a-zA-Z]+", question.lower()))
    scored = []
    for item in KB:
        score = len(words & item["keywords"])
        if score:
            scored.append((score, item))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:2]]


def chatbot_answer(question):
    df = get_active_df(prefer_clean=False)
    issues = STATE["issues"]
    dq = calculate_dq_breakdown(df, issues, STATE["invalid_df"])
    q = question.strip()
    ql = q.lower()
    sql_used = None
    answer = None
    evidence = []

    if df is not None and not df.empty:
        conn = dataframe_to_sql_snapshot(df, issues, dq)
        columns = list(df.columns)
        col = find_column_in_question(q, columns)

        try:
            if any(w in ql for w in ["how many rows", "row count", "records"]):
                sql_used = "SELECT COUNT(*) AS records FROM current_data"
                count = conn.execute(sql_used).fetchone()[0]
                answer = f"The current dataset contains {count:,} records."

            elif "columns" in ql and any(w in ql for w in ["what", "show", "list", "how many"]):
                answer = f"The dataset has {len(columns)} columns: {', '.join(columns)}."

            elif any(w in ql for w in ["missing", "null", "completeness"]):
                missing = df.isna().sum().sort_values(ascending=False)
                top = missing[missing > 0].head(5)
                if len(top):
                    detail = ", ".join([f"{c}: {int(v)}" for c, v in top.items()])
                    answer = f"Completeness is {dq['completeness']}%. Highest missing counts are {detail}."
                else:
                    answer = f"Completeness is {dq['completeness']}%. No null values are currently detected."
                evidence.append("DQ metrics")

            elif "duplicate" in ql:
                selected = [c for c in columns if c.lower() in ql]
                if not selected:
                    duplicate_rules = [r for r in STATE["rules"] if r.get("enabled", True) and r.get("type") == "duplicate"]
                    if duplicate_rules:
                        selected = duplicate_rules[-1]["columns"]
                if not selected:
                    dup_count = int(df.duplicated(keep=False).sum())
                    answer = f"There are {dup_count} rows involved in exact full-row duplicates. Create a Duplicate rule and select the business-key columns for entity-level duplicate detection."
                else:
                    quoted = [f'"{c}"' for c in selected]
                    group_cols = ", ".join(quoted)
                    sql_used = f"SELECT {group_cols}, COUNT(*) AS duplicate_count FROM current_data GROUP BY {group_cols} HAVING COUNT(*) > 1 ORDER BY duplicate_count DESC LIMIT 10"
                    rows = conn.execute(sql_used).fetchall()
                    if rows:
                        answer = f"Using {', '.join(selected)} as the duplicate key, I found {len(rows)} duplicate group(s) in the top results."
                    else:
                        answer = f"No duplicate groups were found using {', '.join(selected)}."
                evidence.append("SQL dataset retrieval")

            elif col and any(w in ql for w in ["average", "mean", "avg"]):
                sql_used = f'SELECT AVG(CAST("{col}" AS REAL)) FROM current_data'
                value = conn.execute(sql_used).fetchone()[0]
                answer = f"The average {col} is {value:.2f}." if value is not None else f"I could not calculate an average for {col}."
                evidence.append("SQL dataset retrieval")

            elif col and "max" in ql:
                sql_used = f'SELECT MAX("{col}") FROM current_data'
                value = conn.execute(sql_used).fetchone()[0]
                answer = f"The maximum {col} is {value}."
                evidence.append("SQL dataset retrieval")

            elif col and "min" in ql:
                sql_used = f'SELECT MIN("{col}") FROM current_data'
                value = conn.execute(sql_used).fetchone()[0]
                answer = f"The minimum {col} is {value}."
                evidence.append("SQL dataset retrieval")

            elif "dq" in ql or "quality score" in ql or "data quality" in ql:
                answer = (
                    f"Overall DQ is {dq['overall']}%. Completeness {dq['completeness']}%, "
                    f"Validity {dq['validity']}%, Uniqueness {dq['uniqueness']}%, "
                    f"Consistency {dq['consistency']}%, Integrity {dq['integrity']}%. "
                    f"Recommended next step: {dq['tips'][0]}"
                )
                evidence.append("DQ metrics")

            elif "issue" in ql or "invalid" in ql or "quarantine" in ql:
                invalid_count = 0 if STATE["invalid_df"] is None else len(STATE["invalid_df"])
                answer = f"The current validation result has {len(issues)} issue group(s) and {invalid_count} invalid/quarantined record(s)."
                if issues:
                    top = sorted(issues, key=lambda x: x.get("rows", 0), reverse=True)[0]
                    answer += f" The largest issue is {top['issue_type']} on {top['column']} affecting {top['rows']} row(s)."
                evidence.append("Validation results")

            elif col and any(w in ql for w in ["top", "common", "frequent"]):
                sql_used = f'SELECT "{col}", COUNT(*) AS n FROM current_data GROUP BY "{col}" ORDER BY n DESC LIMIT 5'
                rows = conn.execute(sql_used).fetchall()
                answer = f"Top {col} values: " + ", ".join([f"{r[0]} ({r[1]})" for r in rows]) + "."
                evidence.append("SQL dataset retrieval")
        finally:
            conn.close()

    kb = retrieve_kb(q)
    if answer is None and kb:
        answer = kb[0]["text"]
        evidence.append(f"RAG knowledge: {kb[0]['title']}")
        if df is not None and any(k in ql for k in ["improve", "increase", "better"]):
            dimension = kb[0]["title"].lower()
            matching_tip = next((t for t in dq["tips"] if t.lower().startswith(dimension)), dq["tips"][0])
            answer += " For this dataset, " + matching_tip
            evidence.append("Current DQ metrics")

    if answer is None:
        if df is None:
            answer = "Load a dataset first. Then I can answer questions about columns, missing values, duplicates, DQ, issues, averages and validation results."
        else:
            answer = "I can currently answer dataset questions about rows, columns, missing values, duplicates, DQ scores, issues, averages, min/max, top values, Spark partitions and Kafka monitoring."

    return {"answer": answer, "sql": sql_used, "evidence": evidence}


def dataframe_export_bytes(df, fmt):
    if df is None:
        return None, None, None
    fmt = fmt.lower()
    if fmt == "csv":
        data = df.to_csv(index=False).encode("utf-8")
        return io.BytesIO(data), "text/csv", "csv"
    if fmt == "json":
        data = df.to_json(orient="records", indent=2, date_format="iso").encode("utf-8")
        return io.BytesIO(data), "application/json", "json"
    if fmt == "xlsx":
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="data")
        buf.seek(0)
        return buf, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"
    return None, None, None


def export_object(kind):
    if kind == "original":
        return STATE["df"]
    if kind == "clean":
        return STATE["clean_df"]
    if kind == "valid":
        return STATE["valid_df"]
    if kind in {"invalid", "quarantine"}:
        return STATE["invalid_df"] if kind == "invalid" else STATE["quarantine_df"]
    if kind == "temp_valid":
        return STATE["temp_valid_df"]
    if kind == "temp_invalid":
        return STATE["temp_invalid_df"]
    if kind == "issues":
        return pd.DataFrame(STATE["issues"])
    if kind == "rules":
        return pd.DataFrame(STATE["rules"])
    if kind == "runs":
        return pd.DataFrame(STATE["runs"])
    if kind == "profile":
        return pd.DataFrame(profile_dataframe(STATE["df"])["columns"])
    if kind == "dq":
        dq = calculate_dq_breakdown(STATE["df"], STATE["issues"], STATE["invalid_df"])
        return pd.DataFrame([{k: v for k, v in dq.items() if k != "tips"}])
    if kind == "partitions":
        parts, _ = compute_partitions(STATE["df"], STATE["partition_count"], STATE["partition_key"])
        return pd.DataFrame(parts)
    if kind == "kafka_events":
        return pd.DataFrame(STATE["stream_events"])
    if kind == "performance":
        profile = profile_dataframe(STATE["df"])["summary"] if STATE["df"] is not None else {}
        _, skew = compute_partitions(STATE["df"], STATE["partition_count"], STATE["partition_key"])
        metrics = {
            "ingestion_ms": STATE["ingestion_ms"],
            "validation_ms": STATE["validation_ms"],
            "last_action_ms": STATE["last_action_ms"],
            "rows_per_sec": STATE["last_rows_per_sec"],
            "memory_mb": profile.get("memory_mb", 0),
            "rows": profile.get("rows", 0),
            "columns": profile.get("columns", 0),
            "spark_partitions": STATE["partition_count"],
            "spark_skew_ratio": skew,
            "kafka_events_per_sec": STATE["stream_rate"],
            "kafka_total_lag": int(sum(STATE["kafka_lag"])),
        }
        return pd.DataFrame([metrics])
    return None


@app.route("/")
def index():
    return render_template("index.html")



def _is_missing_mask(series):
    mask = series.isna()
    if series.dtype == "object":
        mask |= series.astype(str).str.strip().eq("")
    return mask.fillna(False)


def _issue_mask_for(df, issue_id):
    if df is None:
        return pd.Series(dtype=bool)
    if issue_id == "DEFAULT_DUPLICATE_ROW":
        return df.duplicated(keep=False)
    if issue_id == "DEFAULT_ORDER_KEY" and "orderID" in df.columns:
        return df["orderID"].duplicated(keep=False) & df["orderID"].notna()
    if issue_id == "DEFAULT_QUANTITY" and "quantity" in df.columns:
        return pd.to_numeric(df["quantity"], errors="coerce").le(0).fillna(False)
    if issue_id == "DEFAULT_STATUS" and "status" in df.columns:
        allowed = {"NEW", "PAID", "SHIPPED", "CANCELLED"}
        return (~df["status"].isin(allowed) & df["status"].notna()).fillna(False)
    if issue_id == "DEFAULT_NEGATIVE":
        # This default issue is emitted per numeric column.
        return pd.Series(False, index=df.index)
    if issue_id == "DEFAULT_NULL":
        return pd.Series(False, index=df.index)
    for rule in STATE.get("rules", []):
        if rule.get("id") == issue_id:
            _, mask = evaluate_custom_rules(df, [rule])
            return mask.reindex(df.index, fill_value=False).fillna(False)
    return pd.Series(False, index=df.index)


def _issue_mask(df, issue):
    issue_id = issue.get("rule_id") or issue.get("id")
    mask = _issue_mask_for(df, issue_id)
    issue_type = issue.get("issue_type", "")
    col = issue.get("column")
    if issue_id == "DEFAULT_NULL" and col in df.columns:
        return _is_missing_mask(df[col])
    if issue_id == "DEFAULT_NEGATIVE" and col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").lt(0).fillna(False)
    if issue_type == "UNIQUE" and col in df.columns:
        return (df[col].duplicated(keep=False) & df[col].notna()).fillna(False)
    return mask


def _issue_options(df, issue):
    issue_type = issue.get("issue_type", "")
    col = issue.get("column")
    dtype = str(df[col].dtype) if df is not None and col in df.columns else ""
    numeric = bool(df is not None and col in df.columns and pd.api.types.is_numeric_dtype(df[col]))
    if issue_type in {"NULL", "REQUIRED"}:
        base = [
            ("mode", "Most frequent value", "Fill with the most common value"),
            ("custom", "Replace with a value", "Enter the value to use"),
            ("drop", "Remove affected rows", "Delete rows containing missing data"),
            ("quarantine", "Quarantine rows", "Remove from working data and isolate safely"),
            ("leave", "Leave unchanged", "Do not modify this issue"),
        ]
        if numeric:
            base.insert(0, ("mean", "Fill with mean", "Use the column average"))
            base.insert(1, ("median", "Fill with median", "Use the column median"))
        return base, ("median" if numeric else "mode")
    if issue_type in {"NEGATIVE_VALUE", "NON_NEGATIVE"}:
        return [
            ("zero", "Replace with 0", "Set negative values to zero"),
            ("abs", "Use absolute value", "Convert -25 to 25"),
            ("mean", "Replace with mean", "Use the column average"),
            ("median", "Replace with median", "Use the column median"),
            ("drop", "Remove affected rows", "Delete rows with negative values"),
            ("quarantine", "Quarantine rows", "Isolate affected rows"),
            ("leave", "Leave unchanged", "Do not modify this issue"),
        ], "quarantine"
    if issue_type in {"DUPLICATE_ROW", "DUPLICATE_KEY", "DUPLICATE", "UNIQUE"}:
        return [
            ("keep_first", "Keep first", "Keep the first occurrence"),
            ("keep_last", "Keep last", "Keep the latest occurrence"),
            ("remove_all", "Remove duplicates", "Remove every row in the duplicate group"),
            ("quarantine", "Quarantine duplicates", "Move duplicate rows to quarantine"),
            ("leave", "Leave unchanged", "Do not modify duplicates"),
        ], "keep_first"
    if issue_type == "ALLOWED_VALUES":
        return [
            ("mode", "Use most frequent valid value", "Replace invalid values with the most common valid value"),
            ("custom", "Replace with a value", "Enter a valid replacement"),
            ("drop", "Remove affected rows", "Delete rows containing invalid values"),
            ("quarantine", "Quarantine rows", "Isolate invalid rows"),
            ("leave", "Leave unchanged", "Do not modify this issue"),
        ], "quarantine"
    if issue_type == "BUSINESS_RULE":
        return [
            ("one", "Replace with 1", "Use 1 for non-positive quantity"),
            ("median", "Replace with median", "Use the column median"),
            ("custom", "Replace with a value", "Enter the replacement value"),
            ("drop", "Remove affected rows", "Delete invalid rows"),
            ("quarantine", "Quarantine rows", "Isolate invalid rows"),
            ("leave", "Leave unchanged", "Do not modify this issue"),
        ], "one"
    if issue_type == "RANGE":
        return [
            ("clip", "Clamp to allowed range", "Move values back inside the configured limits"),
            ("median", "Replace with median", "Use the column median"),
            ("drop", "Remove affected rows", "Delete out-of-range rows"),
            ("quarantine", "Quarantine rows", "Isolate out-of-range rows"),
            ("leave", "Leave unchanged", "Do not modify this issue"),
        ], "clip"
    if issue_type == "DATATYPE":
        return [
            ("convert", "Convert values", "Coerce values into the expected datatype"),
            ("drop", "Remove affected rows", "Delete values that cannot be converted"),
            ("quarantine", "Quarantine rows", "Isolate rows that fail conversion"),
            ("leave", "Leave unchanged", "Do not modify this issue"),
        ], "convert"
    return [
        ("quarantine", "Quarantine rows", "Isolate affected rows"),
        ("drop", "Remove affected rows", "Delete affected rows"),
        ("leave", "Leave unchanged", "Do not modify this issue"),
    ], "quarantine"


def _scan_issue_details(df):
    default_issues, _ = default_validation(df)
    custom_issues, _ = evaluate_custom_rules(df)
    issues = default_issues + custom_issues
    enriched = []
    for idx, issue in enumerate(issues):
        item = dict(issue)
        item["ui_id"] = f"DQ-{idx+1}"
        columns = item.get("columns") or ([item.get("column")] if item.get("column") not in (None, "ALL") else list(df.columns))
        item["columns"] = [c for c in columns if c in df.columns]
        primary = item["columns"][0] if item["columns"] else None
        if primary and primary in df.columns:
            values = df.loc[_issue_mask(df, item), primary].head(5).tolist()
            item["dtype"] = str(df[primary].dtype)
            item["sample_values"] = [json_safe(v) for v in values]
        else:
            item["dtype"] = "mixed"
            item["sample_values"] = []
        mask = _issue_mask(df, item).fillna(False)
        affected = df.loc[mask].copy()
        rows_data = []
        primary_col = primary
        for position, (row_idx, row) in enumerate(affected.head(25).iterrows(), start=1):
            record = {k: json_safe(v) for k, v in row.to_dict().items()}
            rows_data.append({
                "row_number": position,
                "row_index": json_safe(row_idx),
                "issue_column": primary_col,
                "issue_value": json_safe(row.get(primary_col)) if primary_col else None,
                "data": record,
            })
        item["affected_rows"] = rows_data
        item["affected_rows_total"] = int(mask.sum())
        options, recommended = _issue_options(df, item)
        option_payload = []
        for a, b, c in options:
            preview = None
            if primary and primary in df.columns and a in {"mean", "median", "mode"}:
                series = pd.to_numeric(df[primary], errors="coerce")
                if a == "mean":
                    value = series.mean()
                elif a == "median":
                    value = series.median()
                else:
                    mode = df[primary].mode(dropna=True)
                    value = mode.iloc[0] if not mode.empty else None
                preview = json_safe(value)
            option_payload.append({"id": a, "label": b, "help": c, "preview": preview})
        item["options"] = option_payload
        item["recommended"] = recommended
        item["fixable"] = any(o[0] != "leave" for o in options)
        enriched.append(item)
    return enriched


def _apply_clean_actions(df, actions):
    working = df.copy()
    quarantined_parts = []
    changes = []
    dropped_indices = set()

    def add_change(action, column, count, detail):
        if count:
            changes.append({"action": action, "column": column or "ALL", "rows": int(count), "detail": detail})

    for action in actions:
        operation = action.get("operation", "leave")
        if operation == "leave":
            continue
        issue_id = action.get("issue_id")
        action_columns = action.get("columns") or []
        issue = next((x for x in _scan_issue_details(working)
                      if x.get("rule_id") == issue_id and
                      (not action_columns or set(x.get("columns") or []) == set(action_columns))), None)
        if issue is None:
            continue
        cols = [c for c in (action.get("columns") or issue.get("columns") or []) if c in working.columns]
        col = action.get("column") or (cols[0] if cols else None)
        mask = _issue_mask(working, issue).fillna(False)
        if not bool(mask.any()):
            continue

        if operation in {"drop", "quarantine"}:
            affected = working[mask].copy()
            if operation == "quarantine":
                affected["_quarantine_timestamp"] = now_text()
                affected["_reason"] = issue.get("reason", "User-selected quarantine")
                affected["_issue_groups"] = 1
                quarantined_parts.append(affected)
            add_change(operation, col, len(affected), issue.get("reason", "Rows affected"))
            working = working.loc[~mask].copy()
            continue

        if operation in {"keep_first", "keep_last", "remove_all"}:
            subset = cols or list(working.columns)
            if operation == "keep_first":
                dup = working.duplicated(subset=subset, keep="first")
                target = mask & dup
            elif operation == "keep_last":
                dup = working.duplicated(subset=subset, keep="last")
                target = mask & dup
            else:
                target = mask
            count = int(target.sum())
            working = working.loc[~target].copy()
            add_change(operation, ", ".join(subset), count, "Duplicate handling")
            continue

        if col is None or col not in working.columns:
            continue
        series = working[col]
        old_values = series.loc[mask].copy()
        replacement = action.get("value")
        if operation in {"mean", "median", "mode"}:
            if operation == "mean":
                replacement = pd.to_numeric(series, errors="coerce").mean()
            elif operation == "median":
                replacement = pd.to_numeric(series, errors="coerce").median()
            else:
                mode = series.mode(dropna=True)
                replacement = mode.iloc[0] if not mode.empty else "UNKNOWN"
            if pd.isna(replacement):
                replacement = 0 if pd.api.types.is_numeric_dtype(series) else "UNKNOWN"
            working.loc[mask, col] = replacement
        elif operation == "custom":
            if replacement in (None, ""):
                continue
            working.loc[mask, col] = replacement
        elif operation == "zero":
            working.loc[mask, col] = 0
        elif operation == "abs":
            working.loc[mask, col] = pd.to_numeric(series.loc[mask], errors="coerce").abs()
        elif operation == "one":
            working.loc[mask, col] = 1
        elif operation == "clip":
            rule = next((r for r in STATE.get("rules", []) if r.get("id") == issue_id), None)
            params = (rule or {}).get("params", {})
            values = pd.to_numeric(series, errors="coerce")
            if params.get("min") not in (None, ""):
                values = values.clip(lower=float(params["min"]))
            if params.get("max") not in (None, ""):
                values = values.clip(upper=float(params["max"]))
            working[col] = values
        elif operation == "convert":
            rule = next((r for r in STATE.get("rules", []) if r.get("id") == issue_id), None)
            expected = ((rule or {}).get("params", {})).get("datatype", "string")
            if expected in {"numeric", "integer"}:
                converted = pd.to_numeric(series, errors="coerce")
                if expected == "integer":
                    converted = converted.round()
                working[col] = converted
            elif expected == "date":
                working[col] = pd.to_datetime(series, errors="coerce")
            elif expected == "boolean":
                mapping = {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
                working[col] = series.astype(str).str.lower().map(mapping)
            else:
                working[col] = series.astype(str)
        else:
            continue
        changed = int((old_values.astype(str) != working.loc[old_values.index, col].astype(str)).sum())
        add_change(operation, col, changed, f"Updated {changed} value(s)")

    quarantine = pd.concat(quarantined_parts, ignore_index=True) if quarantined_parts else pd.DataFrame(columns=list(df.columns) + ["_quarantine_timestamp", "_reason", "_issue_groups"])
    return working.reset_index(drop=True), quarantine, changes




def _smart_relationships(source_ids):
    """Discover likely relationships without requiring users to understand joins."""
    pairs = []
    for i, lid in enumerate(source_ids):
        left = _integration_source_record(lid)
        if not left:
            continue
        for rid in source_ids[i+1:]:
            right = _integration_source_record(rid)
            if not right:
                continue
            candidates = _join_key_candidates(left["df"], right["df"])
            for c in candidates[:8]:
                # Existing candidate score already combines name/type/value evidence.
                confidence = float(c.get("score", 0))
                pairs.append({
                    "left_source_id": lid,
                    "left_name": left["name"],
                    "left_key": c["left"],
                    "right_source_id": rid,
                    "right_name": right["name"],
                    "right_key": c["right"],
                    "confidence": round(confidence * 100, 1),
                    "reason": c.get("reason", "Matching key candidate"),
                    "join_type": "left",
                })
    # Keep the strongest relationship for each source pair.
    pairs.sort(key=lambda x: x["confidence"], reverse=True)
    chosen_pairs = set()
    unique = []
    for x in pairs:
        pair = tuple(sorted((x["left_source_id"], x["right_source_id"])))
        if pair in chosen_pairs:
            continue
        chosen_pairs.add(pair)
        unique.append(x)
    return unique


@app.route("/api/integration/smart/analyze", methods=["POST"])
def api_smart_integration_analyze():
    body = request.get_json(silent=True) or {}
    source_ids = body.get("source_ids") or _integration_state().get("source_ids") or list(STATE.get("sources", {}).keys())
    source_ids = [x for x in source_ids if x in STATE.get("sources", {})]
    if len(source_ids) < 2:
        return jsonify({"ok": False, "error": "Add at least two datasets before Smart Integration."}), 400
    rels = _smart_relationships(source_ids)
    auto = [r for r in rels if r["confidence"] >= 92]
    review = [r for r in rels if 70 <= r["confidence"] < 92]
    low = [r for r in rels if r["confidence"] < 70]
    return jsonify({"ok": True, "relationships": rels, "auto": auto, "review": review, "low": low,
                    "source_ids": source_ids,
                    "message": f"Found {len(rels)} likely relationship(s)."})


@app.route("/api/integration/smart/build", methods=["POST"])
def api_smart_integration_build():
    body = request.get_json(silent=True) or {}
    source_ids = body.get("source_ids") or _integration_state().get("source_ids") or list(STATE.get("sources", {}).keys())
    source_ids = [x for x in source_ids if x in STATE.get("sources", {})]
    if len(source_ids) < 2:
        return jsonify({"ok": False, "error": "Add at least two datasets before Smart Integration."}), 400
    project_name = (body.get("project_name") or _integration_state().get("project_name") or "Sales Analysis").strip()
    rels = _smart_relationships(source_ids)
    auto = [r for r in rels if r["confidence"] >= 92]
    if not auto:
        return jsonify({"ok": False, "error": "No high-confidence relationship was found. Review the suggested relationships or use Advanced Integration."}), 400

    # Use the largest source as the fact/base dataset. This preserves its rows.
    base_id = max(source_ids, key=lambda sid: len(STATE["sources"][sid]["df"]))
    # Build a graph outward from the base source.
    steps = []
    used = {base_id}
    pending = True
    while pending:
        pending = False
        for r in auto:
            a, b = r["left_source_id"], r["right_source_id"]
            if a in used and b not in used:
                steps.append({"right_source_id": b, "left_key": r["left_key"], "right_key": r["right_key"], "how": "left", "normalize": True, "suffix": f"src{len(steps)+1}"})
                used.add(b); pending = True
            elif b in used and a not in used:
                steps.append({"right_source_id": a, "left_key": r["right_key"], "right_key": r["left_key"], "how": "left", "normalize": True, "suffix": f"src{len(steps)+1}"})
                used.add(a); pending = True
    if not steps:
        return jsonify({"ok": False, "error": "The detected relationships do not connect the selected datasets."}), 400

    st = _integration_state()
    st.update({"project_name": project_name, "source_ids": source_ids, "base_source_id": base_id, "steps": steps,
               "combined_df": None, "report": {}, "normalized": {}})
    try:
        combined, detailed = _build_integration_preview(st)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    st["combined_df"] = combined
    st["steps"] = detailed
    st["report"] = _integration_report(combined, detailed, project_name)

    generated_name = _slug_source_name(project_name + "_combined")
    for sid, src in list(STATE.get("sources", {}).items()):
        if src.get("details", {}).get("generated_from_project") == project_name:
            del STATE["sources"][sid]
    record = _register_source(combined, generated_name, "Integrated Dataset",
                              {"generated_from_project": project_name, "joins": len(detailed), "smart_integration": True}, select=True)
    st["combined_source_id"] = record["id"]

    validation_issues, validation_mask = default_validation(combined)
    valid, invalid = split_by_mask(combined, validation_mask)
    STATE["issues"] = validation_issues
    STATE["valid_df"] = valid
    STATE["invalid_df"] = invalid
    STATE["quarantine_df"] = add_quarantine_metadata(invalid, validation_issues, "Integration validation failure")
    dq = calculate_dq_breakdown(combined, validation_issues, invalid)
    st["report"]["validation"] = {"valid": len(valid), "invalid": len(invalid), "quarantine": len(STATE["quarantine_df"]), "issue_groups": len(validation_issues), "dq_score": dq["overall"]}
    st["report"]["smart"] = True
    st["report"]["unconnected_sources"] = [STATE["sources"][sid]["name"] for sid in source_ids if sid not in used]
    st["report"]["relationship_confidence"] = [{"left": r["left_name"], "left_key": r["left_key"], "right": r["right_name"], "right_key": r["right_key"], "confidence": r["confidence"]} for r in rels]
    add_run("Smart Integrated", combined, validation_issues, "integration")
    return jsonify({"ok": True, "source": {k:v for k,v in record.items() if k != "df"}, "report": st["report"],
                    "preview": dataframe_records(combined, 25), "rows": len(combined), "columns": list(combined.columns),
                    "valid": len(valid), "invalid": len(invalid), "quarantine": len(STATE["quarantine_df"]),
                    "relationships": rels, "auto_count": len(auto), "unconnected": st["report"]["unconnected_sources"]})


@app.route("/api/integration/state")
def api_integration_state():
    st = _integration_state()
    sources = []
    for sid in st.get("source_ids", []):
        src = _integration_source_record(sid)
        if src:
            sources.append({k: v for k, v in src.items() if k != "df"})
    candidates = []
    if st.get("base_source_id"):
        base = _integration_source_record(st["base_source_id"])
        for src in sources:
            if src["id"] != st["base_source_id"]:
                other = _integration_source_record(src["id"])
                if base and other:
                    candidates.extend([{**c, "right_source_id": other["id"], "right_source_name": other["name"]} for c in _join_key_candidates(base["df"], other["df"])])
    return jsonify({"ok": True, "project_name": st.get("project_name"), "source_ids": st.get("source_ids", []),
                    "base_source_id": st.get("base_source_id"), "sources": sources, "steps": st.get("steps", []),
                    "report": st.get("report", {}), "has_combined": st.get("combined_df") is not None,
                    "combined_preview": dataframe_records(st.get("combined_df"), 15), "candidates": candidates[:20]})


@app.route("/api/integration/project", methods=["POST"])
def api_integration_project():
    body = request.get_json(force=True) or {}
    name = (body.get("name") or "Sales Analysis").strip()
    if not name:
        return jsonify({"ok": False, "error": "Project name is required"}), 400
    st = _integration_state()
    st.update({"project_name": name, "source_ids": [], "base_source_id": None, "steps": [], "combined_df": None, "report": {}, "normalized": {}})
    return jsonify({"ok": True, "project_name": name})


@app.route("/api/integration/sources", methods=["POST"])
def api_integration_sources():
    body = request.get_json(force=True) or {}
    ids = body.get("source_ids") or []
    if len(ids) < 2:
        return jsonify({"ok": False, "error": "Select at least two datasets to integrate"}), 400
    missing = [sid for sid in ids if sid not in STATE.get("sources", {})]
    if missing:
        return jsonify({"ok": False, "error": f"Source not found: {missing[0]}"}), 404
    st = _integration_state()
    st["source_ids"] = ids
    st["base_source_id"] = body.get("base_source_id") or ids[0]
    st["steps"] = []
    st["combined_df"] = None
    st["report"] = {}
    return jsonify({"ok": True, "source_ids": ids, "base_source_id": st["base_source_id"]})


@app.route("/api/integration/candidates")
def api_integration_candidates():
    left_id = request.args.get("left")
    right_id = request.args.get("right")
    st = _integration_state()
    if left_id == "__combined__":
        left_df = st.get("combined_df")
        left_name = "Current Combined Result"
    else:
        left = _integration_source_record(left_id)
        left_df = left["df"] if left else None
        left_name = left.get("name") if left else None
    right = _integration_source_record(right_id)
    if left_df is None or not right:
        return jsonify({"ok": False, "error": "Choose two valid datasets"}), 400
    candidates = _join_key_candidates(left_df, right["df"])
    return jsonify({"ok": True, "left_name": left_name, "right_name": right["name"], "candidates": candidates,
                    "left_columns": [{"name": c, "dtype": str(left_df[c].dtype)} for c in left_df.columns],
                    "right_columns": [{"name": c, "dtype": str(right["df"][c].dtype)} for c in right["df"].columns]})


@app.route("/api/integration/join", methods=["POST"])
def api_integration_join():
    body = request.get_json(force=True) or {}
    st = _integration_state()
    if not st.get("project_name"):
        return jsonify({"ok": False, "error": "Create an integration project first"}), 400
    if not st.get("source_ids"):
        return jsonify({"ok": False, "error": "Select datasets for the project first"}), 400
    right_id = body.get("right_source_id")
    right = _integration_source_record(right_id)
    if not right:
        return jsonify({"ok": False, "error": "Right dataset not found"}), 404
    left_key = body.get("left_key")
    right_key = body.get("right_key")
    if not left_key or not right_key:
        return jsonify({"ok": False, "error": "Choose both join columns"}), 400
    # Current preview is rebuilt from all saved steps, so adding a step is deterministic.
    step = {"right_source_id": right_id, "left_key": left_key, "right_key": right_key,
            "how": body.get("how") or "left", "normalize": bool(body.get("normalize", True)),
            "suffix": body.get("suffix") or f"src{len(st.get('steps', []))+1}"}
    st["steps"].append(step)
    try:
        combined, detailed = _build_integration_preview(st)
    except Exception as exc:
        st["steps"].pop()
        return jsonify({"ok": False, "error": str(exc)}), 400
    st["combined_df"] = combined
    st["report"] = _integration_report(combined, detailed, st["project_name"])
    st["steps"] = detailed
    return jsonify({"ok": True, "steps": detailed, "report": st["report"], "preview": dataframe_records(combined, 20)})


@app.route("/api/integration/build", methods=["POST"])
def api_integration_build():
    st = _integration_state()
    if not st.get("project_name") or not st.get("steps"):
        return jsonify({"ok": False, "error": "Create a project, select datasets and add at least one join"}), 400
    try:
        combined, detailed = _build_integration_preview(st)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    st["combined_df"] = combined
    st["steps"] = detailed
    st["report"] = _integration_report(combined, detailed, st["project_name"])
    generated_name = _slug_source_name(st["project_name"] + "_combined")
    # Replace an older generated dataset with the same logical project name.
    for sid, src in list(STATE.get("sources", {}).items()):
        if src.get("details", {}).get("generated_from_project") == st["project_name"]:
            del STATE["sources"][sid]
    record = _register_source(combined, generated_name, "Integrated Dataset",
                              {"generated_from_project": st["project_name"], "joins": len(detailed)}, select=True)
    st["combined_source_id"] = record["id"]
    # Give the newly integrated dataset an immediate quality report. This does not modify
    # the generated source; it only classifies rows and creates the standard quarantine view.
    validation_issues, validation_mask = default_validation(combined)
    valid, invalid = split_by_mask(combined, validation_mask)
    STATE["issues"] = validation_issues
    STATE["valid_df"] = valid
    STATE["invalid_df"] = invalid
    STATE["quarantine_df"] = add_quarantine_metadata(invalid, validation_issues, "Integration validation failure")
    dq = calculate_dq_breakdown(combined, validation_issues, invalid)
    st["report"]["validation"] = {"valid": len(valid), "invalid": len(invalid), "quarantine": len(STATE["quarantine_df"]), "issue_groups": len(validation_issues), "dq_score": dq["overall"]}
    add_run("Integrated", combined, validation_issues, "integration")
    return jsonify({"ok": True, "source": {k: v for k, v in record.items() if k != "df"}, "report": st["report"],
                    "preview": dataframe_records(combined, 25), "rows": len(combined), "columns": list(combined.columns),
                    "valid": len(valid), "invalid": len(invalid), "quarantine": len(STATE["quarantine_df"])})


@app.route("/api/integration/reset", methods=["POST"])
def api_integration_reset():
    st = _integration_state()
    for sid, src in list(STATE.get("sources", {}).items()):
        if src.get("details", {}).get("generated_from_project"):
            del STATE["sources"][sid]
    st.update({"project_name": None, "source_ids": [], "base_source_id": None, "steps": [], "combined_df": None, "report": {}, "normalized": {}})
    if STATE.get("active_source_id") not in STATE.get("sources", {}):
        if STATE.get("sources"):
            _select_source(next(iter(STATE["sources"])))
        else:
            STATE["active_source_id"] = None; STATE["df"] = None; STATE["source_name"] = None; _reset_dataset_state()
    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    df = get_active_df()
    invalid = 0 if STATE["invalid_df"] is None else len(STATE["invalid_df"])
    valid = 0 if STATE["valid_df"] is None else len(STATE["valid_df"])
    dq = calculate_dq_breakdown(STATE["df"], STATE["issues"], STATE["invalid_df"])
    return jsonify({
        "records": 0 if df is None else len(df),
        "valid": valid,
        "invalid": invalid,
        "quarantined": 0 if STATE["quarantine_df"] is None else len(STATE["quarantine_df"]),
        "quality_score": dq["overall"],
        "source_name": STATE["source_name"],
        "kafka_connected": STATE["kafka_connected"],
        "spark_connected": STATE["spark_connected"],
        "stream_running": STATE["stream_running"],
        "rules": len(STATE["rules"]),
        "source_count": len(STATE.get("sources", {})),
        "active_source_id": STATE.get("active_source_id"),
    })


@app.route("/api/demo", methods=["POST"])
def api_demo():
    start = time.perf_counter()
    df = make_demo_orders()
    record = _register_source(df, "demo_orders", "Demo", {"filename": "demo_orders"}, select=True)
    STATE["ingestion_ms"] = round((time.perf_counter() - start) * 1000, 2)
    update_performance(start, len(df))
    return jsonify({"ok": True, "source": {k: v for k, v in record.items() if k != "df"}, "rows": len(df), "columns": list(df.columns)})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    files = request.files.getlist("files") or request.files.getlist("file")
    files = [f for f in files if f and f.filename]
    if not files:
        return jsonify({"ok": False, "error": "Choose at least one file"}), 400
    start = time.perf_counter()
    added = []
    try:
        for file in files:
            loaded, source_type = _read_uploaded_source(file)
            dataset_name = (request.form.get("dataset_name") or "").strip() if len(files) == 1 else ""

            # A SQL dump may contain several tables. Register every table as
            # its own dataset so relational data can be validated independently.
            if source_type == "SQL Dump":
                for table_name, df in loaded:
                    base_name = dataset_name if dataset_name and len(loaded) == 1 else f"{file.filename.rsplit('.', 1)[0]}_{table_name}"
                    record = _register_source(
                        df,
                        base_name,
                        "SQL Table",
                        {"filename": file.filename, "table": table_name, "format": "SQL dump"},
                        select=False,
                    )
                    added.append({k: v for k, v in record.items() if k != "df"})
            else:
                df = loaded
                name = dataset_name or file.filename.rsplit(".", 1)[0]
                record = _register_source(df, name, source_type, {"filename": file.filename}, select=False)
                added.append({k: v for k, v in record.items() if k != "df"})

        if not added:
            raise ValueError("No datasets were found in the uploaded source")
        _select_source(added[-1]["id"])
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    elapsed = update_performance(start, sum(x["rows"] for x in added))
    STATE["ingestion_ms"] = round(elapsed, 2)
    return jsonify({"ok": True, "sources": _source_summaries(), "active_source_id": STATE["active_source_id"], "rows": len(STATE["df"]), "columns": list(STATE["df"].columns), "preview": dataframe_records(STATE["df"], 20)})


@app.route("/api/sources")
def api_sources():
    return jsonify({"sources": _source_summaries(), "active_source_id": STATE.get("active_source_id")})


@app.route("/api/sources/select/<source_id>", methods=["POST"])
def api_select_source(source_id):
    if not _select_source(source_id):
        return jsonify({"ok": False, "error": "Source not found"}), 404
    return jsonify({"ok": True, "active_source_id": source_id, "source_name": STATE["source_name"], "rows": len(STATE["df"]), "columns": list(STATE["df"].columns)})


@app.route("/api/sources/<source_id>", methods=["DELETE"])
def api_remove_source(source_id):
    if source_id not in STATE.get("sources", {}):
        return jsonify({"ok": False, "error": "Source not found"}), 404
    was_active = source_id == STATE.get("active_source_id")
    del STATE["sources"][source_id]
    if was_active:
        if STATE["sources"]:
            _select_source(next(iter(STATE["sources"])))
        else:
            STATE["active_source_id"] = None
            STATE["df"] = None
            STATE["source_name"] = None
            _reset_dataset_state()
    return jsonify({"ok": True, "sources": _source_summaries(), "active_source_id": STATE.get("active_source_id")})


@app.route("/api/source/rest", methods=["POST"])
def api_source_rest():
    body = request.get_json(force=True) or {}
    url = (body.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "REST URL is required"}), 400
    start = time.perf_counter()
    try:
        df = _read_rest_json(url)
        name = (body.get("name") or "rest_api_source").strip()
        record = _register_source(df, name, "REST API", {"url": url}, select=True)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    elapsed = update_performance(start, len(df))
    STATE["ingestion_ms"] = round(elapsed, 2)
    return jsonify({"ok": True, "source": {k: v for k, v in record.items() if k != "df"}, "rows": len(df), "columns": list(df.columns), "preview": dataframe_records(df, 20)})


@app.route("/api/preview")
def api_preview():
    view = request.args.get("view", "current")
    mapping = {
        "original": STATE["df"],
        "clean": STATE["clean_df"],
        "valid": STATE["valid_df"],
        "invalid": STATE["invalid_df"],
        "quarantine": STATE["quarantine_df"],
        "temp_valid": STATE["temp_valid_df"],
        "temp_invalid": STATE["temp_invalid_df"],
        "current": get_active_df(),
    }
    df = mapping.get(view)
    return jsonify({"columns": [] if df is None else list(df.columns), "rows": dataframe_records(df, 60)})


@app.route("/api/columns")
def api_columns():
    df = STATE["df"]
    if df is None:
        return jsonify([])
    return jsonify([{"name": c, "dtype": str(df[c].dtype)} for c in df.columns])


@app.route("/api/rules", methods=["GET", "POST"])
def api_rules():
    if request.method == "GET":
        return jsonify(STATE["rules"])
    if STATE["df"] is None:
        return jsonify({"ok": False, "error": "Load a dataset first"}), 400
    body = request.get_json(force=True)
    rule_type = body.get("type")
    columns = body.get("columns") or []
    if not rule_type or not columns:
        return jsonify({"ok": False, "error": "Choose a rule type and at least one column"}), 400
    if rule_type in {"range", "allowed_values", "datatype", "regex", "unique"} and len(columns) > 1:
        return jsonify({"ok": False, "error": f"{rule_type} accepts one column at a time"}), 400
    rule = {
        "id": "RULE-" + datetime.now().strftime("%H%M%S%f")[:10],
        "name": body.get("name") or rule_type.replace("_", " ").title(),
        "type": rule_type,
        "columns": columns,
        "params": body.get("params") or {},
        "severity": body.get("severity", "MEDIUM"),
        "enabled": True,
    }
    STATE["rules"].append(rule)
    return jsonify({"ok": True, "rule": rule})


@app.route("/api/rules/<rule_id>/toggle", methods=["POST"])
def toggle_rule(rule_id):
    for rule in STATE["rules"]:
        if rule["id"] == rule_id:
            rule["enabled"] = not rule.get("enabled", True)
            return jsonify({"ok": True, "enabled": rule["enabled"]})
    return jsonify({"ok": False, "error": "Rule not found"}), 404


@app.route("/api/rules/<rule_id>", methods=["DELETE"])
def delete_rule(rule_id):
    before = len(STATE["rules"])
    STATE["rules"] = [r for r in STATE["rules"] if r["id"] != rule_id]
    return jsonify({"ok": len(STATE["rules"]) < before})


@app.route("/api/validate/<mode>", methods=["POST"])
def api_validate(mode):
    df = STATE["df"]
    if df is None:
        return jsonify({"ok": False, "error": "Load a dataset first"}), 400
    start = time.perf_counter()

    if mode == "default":
        issues, mask = default_validation(df)
        valid, invalid = split_by_mask(df, mask)
        STATE["issues"] = issues
        STATE["valid_df"] = valid
        STATE["invalid_df"] = invalid
        STATE["quarantine_df"] = add_quarantine_metadata(invalid, issues, "Default validation failure")
        elapsed = update_performance(start, len(df))
        STATE["validation_ms"] = round(elapsed, 2)
        add_run("Validated", df, issues, "default")
        return jsonify({"ok": True, "mode": mode, "issues": issues, "valid": len(valid), "invalid": len(invalid), "dq": calculate_dq_breakdown(df, issues, invalid)})

    if mode == "custom-preview":
        issues, mask = evaluate_custom_rules(df)
        valid, invalid = split_by_mask(df, mask)
        STATE["temp_issues"] = issues
        STATE["temp_valid_df"] = valid
        STATE["temp_invalid_df"] = invalid
        update_performance(start, len(df))
        return jsonify({"ok": True, "mode": mode, "issues": issues, "valid": len(valid), "invalid": len(invalid), "preview": dataframe_records(invalid, 25)})

    if mode == "combined":
        d_issues, d_mask = default_validation(df)
        c_issues, c_mask = evaluate_custom_rules(df)
        mask = d_mask | c_mask
        issues = d_issues + c_issues
        valid, invalid = split_by_mask(df, mask)
        STATE["issues"] = issues
        STATE["valid_df"] = valid
        STATE["invalid_df"] = invalid
        STATE["quarantine_df"] = add_quarantine_metadata(invalid, issues, "Combined validation failure")
        elapsed = update_performance(start, len(df))
        STATE["validation_ms"] = round(elapsed, 2)
        add_run("Validated", df, issues, "combined")
        return jsonify({"ok": True, "mode": mode, "issues": issues, "valid": len(valid), "invalid": len(invalid), "dq": calculate_dq_breakdown(df, issues, invalid)})

    return jsonify({"ok": False, "error": "Unknown validation mode"}), 400


@app.route("/api/autofix", methods=["POST"])
def api_autofix():
    if STATE["df"] is None:
        return jsonify({"ok": False, "error": "Load a dataset first"}), 400
    start = time.perf_counter()
    STATE["clean_df"] = auto_fix(STATE["df"])
    STATE["df"] = STATE["clean_df"].copy()
    _sync_active_source_from_df()
    d_issues, d_mask = default_validation(STATE["clean_df"])
    c_issues, c_mask = evaluate_custom_rules(STATE["clean_df"])
    issues = d_issues + c_issues
    valid, invalid = split_by_mask(STATE["clean_df"], d_mask | c_mask)
    STATE["issues"] = issues
    STATE["valid_df"] = valid
    STATE["invalid_df"] = invalid
    STATE["quarantine_df"] = add_quarantine_metadata(invalid, issues, "Remaining issue after auto-fix")
    update_performance(start, len(STATE["clean_df"]))
    add_run("Auto-fixed", STATE["clean_df"], issues, "combined")
    return jsonify({"ok": True, "issues": issues, "dq": calculate_dq_breakdown(STATE["clean_df"], issues, invalid)})


@app.route("/api/quality/scan")
def api_quality_scan():
    df = get_active_df()
    if df is None:
        return jsonify({"ok": False, "error": "Load a dataset first"}), 400
    issues = _scan_issue_details(df)
    return jsonify({
        "ok": True,
        "rows": len(df),
        "issues": issues,
        "issue_count": len(issues),
        "affected_rows": int(sum(i.get("rows", 0) for i in issues)),
        "source_view": "working" if STATE.get("clean_df") is not None else "dataset",
    })


@app.route("/api/clean/preview", methods=["POST"])
def api_clean_preview():
    if STATE["df"] is None:
        return jsonify({"ok": False, "error": "Load a dataset first"}), 400
    body = request.get_json(force=True) or {}
    actions = body.get("actions") or []
    if not actions:
        return jsonify({"ok": False, "error": "Choose at least one fix first"}), 400
    base_df = get_active_df()
    working, quarantine, changes = _apply_clean_actions(base_df, actions)
    STATE["clean_plan"] = actions
    STATE["clean_preview_df"] = working
    STATE["clean_preview_changes"] = changes
    STATE["clean_quarantine_df"] = quarantine
    return jsonify({
        "ok": True,
        "before_rows": len(base_df),
        "after_rows": len(working),
        "quarantine_rows": len(quarantine),
        "changes": changes,
        "preview": dataframe_records(working, 40),
    })


@app.route("/api/clean/apply", methods=["POST"])
def api_clean_apply():
    if STATE["df"] is None:
        return jsonify({"ok": False, "error": "Load a dataset first"}), 400
    if STATE.get("clean_preview_df") is None:
        return jsonify({"ok": False, "error": "Preview the selected fixes before applying them"}), 400
    start = time.perf_counter()
    previous_quarantine_count = 0 if STATE.get("quarantine_df") is None else len(STATE["quarantine_df"])
    STATE["clean_df"] = STATE["clean_preview_df"].copy()
    STATE["df"] = STATE["clean_df"].copy()
    _sync_active_source_from_df()
    explicit_quarantine = STATE["clean_quarantine_df"].copy() if STATE["clean_quarantine_df"] is not None else pd.DataFrame()
    d_issues, d_mask = default_validation(STATE["clean_df"])
    c_issues, c_mask = evaluate_custom_rules(STATE["clean_df"])
    issues = d_issues + c_issues
    valid, invalid = split_by_mask(STATE["clean_df"], d_mask | c_mask)
    STATE["issues"] = issues
    STATE["valid_df"] = valid
    STATE["invalid_df"] = invalid
    remaining_q = add_quarantine_metadata(invalid, issues, "Remaining issue after selected fixes")
    parts = []
    if explicit_quarantine is not None and not explicit_quarantine.empty:
        parts.append(explicit_quarantine)
    if remaining_q is not None and not remaining_q.empty:
        parts.append(remaining_q)
    STATE["quarantine_df"] = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=list(STATE["clean_df"].columns) + ["_quarantine_timestamp", "_reason", "_issue_groups"])
    STATE["validation_ms"] = round(update_performance(start, len(STATE["clean_df"])), 2)
    add_run("Interactive Clean", STATE["clean_df"], issues, "combined")
    return jsonify({
        "ok": True,
        "rows": len(STATE["clean_df"]),
        "valid": len(valid),
        "invalid": len(invalid),
        "quarantine": len(STATE["quarantine_df"]),
        "issues": issues,
        "dq": calculate_dq_breakdown(STATE["clean_df"], issues, invalid),
        "changes": STATE["clean_preview_changes"],
        "fixed_rows": int(sum(c.get("rows", 0) for c in STATE["clean_preview_changes"] if c.get("action") not in {"quarantine", "drop", "remove_all"})),
        "quarantine_change": len(STATE["quarantine_df"]) - previous_quarantine_count,
    })


@app.route("/api/clean/reset", methods=["POST"])
def api_clean_reset():
    STATE["clean_plan"] = []
    STATE["clean_preview_df"] = None
    STATE["clean_preview_changes"] = []
    STATE["clean_quarantine_df"] = None
    return jsonify({"ok": True})


@app.route("/api/issues")
def api_issues():
    return jsonify(STATE["issues"])


@app.route("/api/temp-issues")
def api_temp_issues():
    return jsonify(STATE["temp_issues"])


@app.route("/api/dq")
def api_dq():
    df = STATE["df"]
    current = get_active_df()
    before = calculate_dq_breakdown(df, STATE["issues"], STATE["invalid_df"])
    if current is df or current is None:
        after = before
    else:
        d_i, d_m = default_validation(current)
        c_i, c_m = evaluate_custom_rules(current)
        _, invalid = split_by_mask(current, d_m | c_m)
        after = calculate_dq_breakdown(current, d_i + c_i, invalid)
    return jsonify({"before": before, "after": after})


@app.route("/api/profile")
def api_profile():
    return jsonify(profile_dataframe(STATE["df"]))


@app.route("/api/visualize")
def api_visualize():
    column = request.args.get("column", "")
    return jsonify(column_visualization(STATE["df"], column))


@app.route("/api/performance")
def api_performance():
    df = STATE["df"]
    profile = profile_dataframe(df)["summary"] if df is not None else {}
    partitions, skew = compute_partitions(df, STATE["partition_count"], STATE["partition_key"])
    rows = 0 if df is None else len(df)
    spark_rows_sec = round(max(STATE["last_rows_per_sec"], rows * max(STATE["partition_count"], 1) / max(STATE["last_action_ms"], 1) * 1000), 1) if rows else 0
    return jsonify({
        "ingestion_ms": STATE["ingestion_ms"],
        "validation_ms": STATE["validation_ms"],
        "last_action_ms": STATE["last_action_ms"],
        "rows_per_sec": STATE["last_rows_per_sec"],
        "memory_mb": profile.get("memory_mb", 0),
        "rows": profile.get("rows", 0),
        "columns": profile.get("columns", 0),
        "spark": {
            "mode": "Demo partition/performance model",
            "partitions": STATE["partition_count"],
            "tasks": STATE["partition_count"],
            "estimated_rows_per_sec": spark_rows_sec,
            "skew_ratio": skew,
            "stage_duration_ms": STATE["last_action_ms"],
        },
        "kafka": {
            "topic": "raw-orders",
            "partitions": len(STATE["kafka_offsets"]),
            "rate_per_sec": STATE["stream_rate"],
            "total_lag": int(sum(STATE["kafka_lag"])),
        },
    })


@app.route("/api/partitions", methods=["GET", "POST"])
def api_partitions():
    if request.method == "POST":
        body = request.get_json(force=True)
        STATE["partition_count"] = max(1, min(int(body.get("count", 4)), 32))
        key = body.get("key") or None
        STATE["partition_key"] = key if STATE["df"] is not None and key in STATE["df"].columns else None
    parts, skew = compute_partitions(STATE["df"], STATE["partition_count"], STATE["partition_key"])
    STATE["partition_stats"] = parts
    return jsonify({
        "partitions": parts,
        "count": STATE["partition_count"],
        "key": STATE["partition_key"],
        "skew_ratio": skew,
        "note": "This visualizes a Spark-style partition/repartition preview. Connect real PySpark later to replace these demo metrics with actual executor metrics.",
    })


@app.route("/api/runs")
def api_runs():
    return jsonify(STATE["runs"])


@app.route("/api/chat", methods=["POST"])
def api_chat():
    body = request.get_json(force=True)
    question = str(body.get("message", "")).strip()
    if not question:
        return jsonify({"ok": False, "error": "Enter a question"}), 400
    result = chatbot_answer(question)
    STATE["chat_history"].append({"time": now_text(), "question": question, **result})
    STATE["chat_history"] = STATE["chat_history"][-50:]
    return jsonify({"ok": True, **result})


@app.route("/api/kafka/toggle", methods=["POST"])
def api_kafka_toggle():
    STATE["kafka_connected"] = not STATE["kafka_connected"]
    if not STATE["kafka_connected"]:
        STATE["stream_running"] = False
    return jsonify({"connected": STATE["kafka_connected"]})


@app.route("/api/spark/toggle", methods=["POST"])
def api_spark_toggle():
    STATE["spark_connected"] = not STATE["spark_connected"]
    if not STATE["spark_connected"]:
        STATE["stream_running"] = False
    return jsonify({"connected": STATE["spark_connected"]})


@app.route("/api/stream/toggle", methods=["POST"])
def api_stream_toggle():
    if not (STATE["kafka_connected"] and STATE["spark_connected"]):
        return jsonify({"ok": False, "error": "Connect Kafka and Spark first"}), 400
    STATE["stream_running"] = not STATE["stream_running"]
    return jsonify({"ok": True, "running": STATE["stream_running"]})


@app.route("/api/stream/tick", methods=["POST"])
def api_stream_tick():
    start = time.perf_counter()
    generated = 0
    if STATE["stream_running"] and STATE["kafka_connected"] and STATE["spark_connected"]:
        generated = random.randint(4, 11)
        for _ in range(generated):
            generate_stream_event()
        elapsed = max(time.perf_counter() - start, 0.001)
        STATE["stream_rate"] = round(generated / elapsed, 1)
        for i in range(len(STATE["kafka_lag"])):
            STATE["kafka_lag"][i] = max(0, STATE["kafka_lag"][i] + random.randint(-2, 3))

    total = max(STATE["stream_processed"], 1)
    quality = round(STATE["stream_valid"] / total * 100, 1) if STATE["stream_processed"] else 0
    partition_rows = []
    for i, offset in enumerate(STATE["kafka_offsets"]):
        partition_rows.append({
            "partition": i,
            "latest_offset": offset,
            "consumer_offset": max(0, offset - STATE["kafka_lag"][i]),
            "lag": STATE["kafka_lag"][i],
        })
    return jsonify({
        "processed": STATE["stream_processed"],
        "valid": STATE["stream_valid"],
        "invalid": STATE["stream_invalid"],
        "quarantined": STATE["stream_quarantined"],
        "quality": quality,
        "rate": STATE["stream_rate"],
        "events": STATE["stream_events"][:30],
        "history": STATE["stream_quality_history"],
        "running": STATE["stream_running"],
        "kafka_partitions": partition_rows,
        "total_lag": int(sum(STATE["kafka_lag"])),
    })


@app.route("/api/stream/inject-error", methods=["POST"])
def api_stream_inject_error():
    if not (STATE["kafka_connected"] and STATE["spark_connected"]):
        return jsonify({"ok": False, "error": "Connect Kafka and Spark first"}), 400
    generate_stream_event(force_error=True)
    return jsonify({"ok": True})


@app.route("/api/stream/clear", methods=["POST"])
def api_stream_clear():
    STATE["stream_processed"] = 0
    STATE["stream_valid"] = 0
    STATE["stream_invalid"] = 0
    STATE["stream_quarantined"] = 0
    STATE["stream_events"] = []
    STATE["stream_quality_history"] = []
    STATE["kafka_offsets"] = [0, 0, 0, 0]
    STATE["kafka_lag"] = [0, 0, 0, 0]
    STATE["stream_rate"] = 0.0
    return jsonify({"ok": True})


@app.route("/api/reset", methods=["POST"])
def api_reset():
    rules = STATE["rules"]
    fresh = {
        "df": None, "clean_df": None, "valid_df": None, "invalid_df": None, "quarantine_df": None,
        "integration": {"project_name": None, "source_ids": [], "base_source_id": None, "steps": [], "combined_df": None, "report": {}, "normalized": {}},
        "issues": [], "temp_valid_df": None, "temp_invalid_df": None, "temp_issues": [],
        "runs": [], "source_name": None, "sources": {}, "active_source_id": None, "ingestion_ms": 0.0, "validation_ms": 0.0,
        "last_action_ms": 0.0, "last_rows_per_sec": 0.0, "partition_count": 4,
        "partition_key": None, "partition_stats": [], "kafka_connected": False,
        "spark_connected": False, "stream_running": False, "stream_processed": 0,
        "stream_valid": 0, "stream_invalid": 0, "stream_quarantined": 0,
        "stream_events": [], "stream_quality_history": [], "kafka_offsets": [0, 0, 0, 0],
        "clean_plan": [], "clean_preview_df": None, "clean_preview_changes": [], "clean_quarantine_df": None,
        "kafka_lag": [0, 0, 0, 0], "stream_rate": 0.0, "chat_history": [],
    }
    STATE.update(fresh)
    STATE["rules"] = rules
    return jsonify({"ok": True})


@app.route("/download/<kind>")
def download_kind(kind):
    fmt = request.args.get("format", "csv").lower()
    df = export_object(kind)
    if df is None:
        return "No data available for this export", 404
    if kind == "rules" and not df.empty:
        for col in ["columns", "params"]:
            if col in df.columns:
                df[col] = df[col].apply(json.dumps)
    buf, mime, ext = dataframe_export_bytes(df, fmt)
    if buf is None:
        return "Unsupported format", 400
    return send_file(buf, mimetype=mime, as_attachment=True, download_name=f"{kind}.{ext}")


@app.route("/download/bundle")
def download_bundle():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for kind in ["original", "clean", "valid", "invalid", "quarantine", "issues", "runs", "profile", "rules", "dq", "performance", "partitions", "kafka_events"]:
            df = export_object(kind)
            if df is not None:
                z.writestr(f"{kind}.csv", df.to_csv(index=False))
        dq = calculate_dq_breakdown(STATE["df"], STATE["issues"], STATE["invalid_df"])
        z.writestr("dq_tips.json", json.dumps(dq, indent=2))
        z.writestr("chat_history.json", json.dumps(STATE["chat_history"], indent=2))
        z.writestr("README.txt", "DataGuard export bundle: original/clean/valid/invalid data, validation results, profiling, rules, DQ metrics and chat history.")
    buf.seek(0)
    return send_file(buf, mimetype="application/zip", as_attachment=True, download_name="dataguard_export_bundle.zip")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
