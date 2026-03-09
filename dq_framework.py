import argparse
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import yaml
from google.cloud import bigquery, pubsub_v1


def resolve_process_date(value: Optional[str]) -> str:
    if value:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def normalize_table_ref(project_id: str, table_ref: str) -> str:
    parts = table_ref.split(".")
    if len(parts) == 2:
        return f"{project_id}.{table_ref}"
    if len(parts) == 3:
        return table_ref
    raise ValueError(f"Invalid table reference: {table_ref}")


def normalize_type(t: str) -> str:
    m = {
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
        "STRUCT": "RECORD",
    }
    t = t.upper().strip()
    return m.get(t, t)


def query_one(client: bigquery.Client, sql: str) -> Dict[str, Any]:
    rows = client.query(sql).result()
    for row in rows:
        return dict(row.items())
    return {}


def ensure_results_table(
    client: bigquery.Client, project_id: str, dataset: str, table: str
) -> None:
    sql = f"""
    CREATE SCHEMA IF NOT EXISTS `{project_id}.{dataset}`;

    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.{table}` (
      run_id STRING,
      process_date DATE,
      checked_at TIMESTAMP,
      table_name STRING,
      check_type STRING,
      check_name STRING,
      status STRING,
      severity STRING,
      metric_value FLOAT64,
      threshold STRING,
      details STRING
    )
    PARTITION BY process_date
    CLUSTER BY table_name, status;
    """
    client.query(sql).result()


def make_result(
    run_id: str,
    process_date: str,
    table_name: str,
    check_type: str,
    check_name: str,
    status: str,
    severity: str,
    metric_value: Optional[float],
    threshold: str,
    details: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "process_date": process_date,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "table_name": table_name,
        "check_type": check_type,
        "check_name": check_name,
        "status": status,
        "severity": severity,
        "metric_value": metric_value,
        "threshold": threshold,
        "details": json.dumps(details, sort_keys=True),
    }


def schema_drift_check(
    client: bigquery.Client,
    run_id: str,
    process_date: str,
    table_fq: str,
    required_cols: Dict[str, str],
    severity: str,
) -> Dict[str, Any]:
    table = client.get_table(table_fq)
    actual = {f.name.lower(): normalize_type(f.field_type) for f in table.schema}

    missing = []
    mismatched = []
    for col, expected in required_cols.items():
        actual_type = actual.get(col.lower())
        expected_type = normalize_type(expected)
        if actual_type is None:
            missing.append(col)
        elif actual_type != expected_type:
            mismatched.append(
                {"column": col, "expected": expected_type, "actual": actual_type}
            )

    status = "PASS" if not missing and not mismatched else "FAIL"
    return make_result(
        run_id,
        process_date,
        table_fq,
        "schema_drift",
        "required_columns",
        status,
        severity,
        None,
        "no_missing_or_mismatch",
        {"missing_columns": missing, "type_mismatches": mismatched},
    )


def null_spike_check(
    client: bigquery.Client,
    run_id: str,
    process_date: str,
    table_fq: str,
    date_column: str,
    base_where: str,
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    column = rule["column"]
    baseline_days = int(rule.get("baseline_days", 7))
    spike_multiplier = float(rule.get("spike_multiplier", 2.0))
    max_allowed_rate = float(rule.get("max_allowed_rate", 1.0))
    severity = str(rule.get("severity", "medium"))

    sql = f"""
    WITH current_day AS (
      SELECT
        SAFE_DIVIDE(COUNTIF({column} IS NULL), COUNT(*)) AS null_rate,
        COUNT(*) AS row_count
      FROM `{table_fq}`
      WHERE DATE({date_column}) = DATE('{process_date}')
        AND ({base_where})
    ),
    baseline AS (
      SELECT AVG(day_null_rate) AS avg_null_rate
      FROM (
        SELECT
          DATE({date_column}) AS d,
          SAFE_DIVIDE(COUNTIF({column} IS NULL), COUNT(*)) AS day_null_rate
        FROM `{table_fq}`
        WHERE DATE({date_column}) BETWEEN DATE_SUB(DATE('{process_date}'), INTERVAL {baseline_days} DAY)
                                      AND DATE_SUB(DATE('{process_date}'), INTERVAL 1 DAY)
          AND ({base_where})
        GROUP BY d
      )
    )
    SELECT
      current_day.null_rate AS current_null_rate,
      current_day.row_count AS row_count,
      COALESCE(baseline.avg_null_rate, 0.0) AS baseline_null_rate
    FROM current_day
    CROSS JOIN baseline
    """
    row = query_one(client, sql)

    row_count = int(row.get("row_count") or 0)
    current_rate = float(row.get("current_null_rate") or 0.0)
    baseline_rate = float(row.get("baseline_null_rate") or 0.0)

    if row_count == 0:
        return make_result(
            run_id,
            process_date,
            table_fq,
            "null_spike",
            f"{column}_null_rate",
            "FAIL",
            severity,
            None,
            "row_count > 0",
            {"reason": "no_rows_for_process_date"},
        )

    threshold = max(max_allowed_rate, baseline_rate * spike_multiplier)
    status = "PASS" if current_rate <= threshold else "FAIL"

    return make_result(
        run_id,
        process_date,
        table_fq,
        "null_spike",
        f"{column}_null_rate",
        status,
        severity,
        current_rate,
        f"<= {threshold:.6f}",
        {
            "current_rate": current_rate,
            "baseline_rate": baseline_rate,
            "spike_multiplier": spike_multiplier,
            "max_allowed_rate": max_allowed_rate,
            "row_count": row_count,
        },
    )


def freshness_check(
    client: bigquery.Client,
    run_id: str,
    process_date: str,
    table_fq: str,
    base_where: str,
    rule: Dict[str, Any],
) -> Dict[str, Any]:
    ts_col = rule["timestamp_column"]
    max_lag_minutes = int(rule.get("max_lag_minutes", 60))
    severity = str(rule.get("severity", "high"))

    sql = f"""
    SELECT MAX({ts_col}) AS max_ts
    FROM `{table_fq}`
    WHERE ({base_where})
    """
    row = query_one(client, sql)
    max_ts = row.get("max_ts")

    if max_ts is None:
        return make_result(
            run_id,
            process_date,
            table_fq,
            "freshness",
            f"{ts_col}_lag_minutes",
            "FAIL",
            severity,
            None,
            f"<= {max_lag_minutes}",
            {"reason": "max_timestamp_is_null"},
        )

    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)

    lag_minutes = (
        datetime.now(timezone.utc) - max_ts.astimezone(timezone.utc)
    ).total_seconds() / 60.0
    status = "PASS" if lag_minutes <= max_lag_minutes else "FAIL"

    return make_result(
        run_id,
        process_date,
        table_fq,
        "freshness",
        f"{ts_col}_lag_minutes",
        status,
        severity,
        lag_minutes,
        f"<= {max_lag_minutes}",
        {"max_timestamp": max_ts.isoformat()},
    )


def publish_alert(topic_path: str, payload: Dict[str, Any]) -> None:
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(topic_path, json.dumps(payload).encode("utf-8")).result(timeout=30)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to dq_config.yaml")
    parser.add_argument("--process_date", help="YYYY-MM-DD")
    parser.add_argument("--alert_topic", help="projects/<project>/topics/<topic>")
    parser.add_argument("--soft_fail", action="store_true", help="Exit 0 even if checks fail")
    args = parser.parse_args()

    process_date = resolve_process_date(args.process_date)

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    project_id = cfg["project_id"]
    monitoring_dataset = cfg.get("monitoring_dataset", "monitoring")
    results_table = cfg.get("results_table", "dq_results")

    bq_client = bigquery.Client(project=project_id)
    ensure_results_table(bq_client, project_id, monitoring_dataset, results_table)

    run_id = str(uuid.uuid4())
    results: List[Dict[str, Any]] = []

    for t in cfg.get("tables", []):
        table_fq = normalize_table_ref(project_id, t["table"])
        date_column = t.get("date_column")
        base_where = t.get("base_where", "TRUE")

        schema_required = t.get("schema_required", {})
        if schema_required:
            res = schema_drift_check(
                bq_client,
                run_id,
                process_date,
                table_fq,
                schema_required,
                str(t.get("schema_severity", "high")),
            )
            results.append(res)

        for rule in t.get("null_spike", []):
            if not date_column:
                raise ValueError(
                    f"date_column missing for table {table_fq} (needed for null_spike)"
                )
            res = null_spike_check(
                bq_client,
                run_id,
                process_date,
                table_fq,
                date_column,
                base_where,
                rule,
            )
            results.append(res)

        freshness_rule = t.get("freshness")
        if freshness_rule:
            res = freshness_check(
                bq_client,
                run_id,
                process_date,
                table_fq,
                base_where,
                freshness_rule,
            )
            results.append(res)

    results_table_fq = f"{project_id}.{monitoring_dataset}.{results_table}"
    errors = bq_client.insert_rows_json(results_table_fq, results)
    if errors:
        raise RuntimeError(f"Failed to write DQ results: {errors}")

    failed = [r for r in results if r["status"] == "FAIL"]
    summary = {
        "run_id": run_id,
        "process_date": process_date,
        "total_checks": len(results),
        "failed_checks": len(failed),
    }
    logging.info("DQ Summary: %s", summary)

    if failed and args.alert_topic:
        publish_alert(
            args.alert_topic,
            {
                **summary,
                "failures": [
                    {
                        "table_name": r["table_name"],
                        "check_type": r["check_type"],
                        "check_name": r["check_name"],
                        "severity": r["severity"],
                        "details": json.loads(r["details"]),
                    }
                    for r in failed
                ],
            },
        )

    if failed and not args.soft_fail:
        raise SystemExit(1)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
