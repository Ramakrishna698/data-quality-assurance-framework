# Data Quality Assurance Framework

Config-driven DQ framework for schema drift, null spikes, and freshness checks across raw and curated layers.

## Stack
Python, BigQuery, Pub/Sub (alerts), YAML config

## Files
- `dq_framework.py`: executes all DQ checks and writes results
- `dq_config.yaml`: rules for tables and thresholds
- `requirements.txt`: Python dependencies
- `sample-data/raw_erp_orders_large.csv`: large mock raw table input
- `sample-data/daily_business_kpis_large.csv`: large mock curated table input

## Checks
- Schema drift against required columns/types
- Null spike detection vs baseline days
- Freshness lag validation

## Output
Results are written to:
`YOUR_PROJECT.monitoring.dq_results`

## Alerting
If `--alert_topic` is provided, failures are published to Pub/Sub.

## Run
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Execute checks:
   ```bash
   python dq_framework.py \
     --config dq_config.yaml \
     --process_date 2026-02-27 \
     --alert_topic projects/YOUR_PROJECT/topics/dq-alerts
   ```

## Exit Behavior
- Exits non-zero when checks fail (default)
- Use `--soft_fail` to force zero exit code


# IN A MORE SIMPLE WAY
# Data Quality Assurance Framework

A Python-based Data Quality Monitoring Framework built for BigQuery.

## Features

- Schema drift detection
- Null spike anomaly detection
- Data freshness monitoring
- Config-driven validation rules
- BigQuery monitoring table
- Pub/Sub alerting for failed checks

## Architecture

BigQuery Tables
        ↓
DQ Framework (Python)
        ↓
Monitoring Table (monitoring.dq_results)
        ↓
Pub/Sub Alerts

## Project Structure

dq_framework.py → Main DQ engine  
dq_config.yaml → Config-driven rules  
sample-data → Example datasets  

## Setup

1. Install dependencies

pip install -r requirements.txt

2. Authenticate GCP

gcloud auth application-default login

3. Run the framework

python dq_framework.py --config dq_config.yaml

## Example Alert

Failed checks trigger Pub/Sub alerts with payload:

{
  "run_id": "...",
  "failed_checks": 5
}
