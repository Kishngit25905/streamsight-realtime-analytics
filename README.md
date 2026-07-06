# StreamSight — Real-Time Event Analytics Platform

A cloud-native, real-time event analytics pipeline built on Google Cloud. StreamSight ingests high-throughput streaming events, processes them with windowed aggregation, persists results to BigQuery, and visualizes live metrics through a Looker Studio dashboard.

---

## Architecture

```
Event Publisher (Python)
        |
        v
  Google Cloud Pub/Sub  (streamsight-events)
        |
        v
  Apache Beam / Google Cloud Dataflow
   - Parse event payloads
   - Fixed windowing (10-sec buckets)
   - Early triggers for live updates
   - Per-window aggregation (count by event_type)
        |
        v
  Google BigQuery  (streamsight_data.event_counts)
        |
        v
  Looker Studio Dashboard
   - Total event scorecard
   - Time series trend
   - Event type breakdown (bar chart)
```

## Technologies Used

- **Google Cloud Pub/Sub** — real-time message ingestion
- **Apache Beam / Google Cloud Dataflow** — stream processing, windowing, aggregation
- **Google BigQuery** — structured storage for analytical querying
- **Looker Studio** — live dashboard visualization
- **Python** — pipeline and event simulation code

## Features

- Simulated real-time event generator (click, page_view, purchase, signup, logout)
- Streaming pipeline with fixed 10-second windows and early/live triggers
- Autoscaling Dataflow deployment (1–100 workers)
- Structured BigQuery schema (`window_start`, `window_end`, `event_type`, `event_count`)
- Live dashboard: total events, trend over time, breakdown by event type
- Cost controls: billing budget alert, soft-delete disabled, clean job shutdown process

## Project Structure

```
StreamSight/
├── publisher.py              # Simulates and publishes fake events to Pub/Sub
├── beam_test.py               # Local Beam pipeline test (no windowing)
├── beam_windowing_test.py     # Local Beam pipeline test (with windowing)
├── streaming_pipeline.py      # Full pipeline: Pub/Sub -> Beam -> BigQuery (local & Dataflow)
└── README.md
```

## Setup & Usage

### Prerequisites
- Google Cloud account with billing enabled (free trial credit is sufficient)
- Python 3.10+, `gcloud` CLI installed and authenticated
- APIs enabled: Pub/Sub, Dataflow, BigQuery, Compute Engine

### 1. Clone and set up environment
```bash
git clone <your-repo-url>
cd StreamSight
python -m venv streamsight-env
streamsight-env\Scripts\activate   # Windows
pip install google-cloud-pubsub google-cloud-bigquery apache-beam[gcp]
```

### 2. Create GCP resources
```bash
gcloud pubsub topics create streamsight-events
gcloud pubsub subscriptions create streamsight-events-sub --topic=streamsight-events

bq mk --dataset --location=<your-region> <project-id>:streamsight_data
bq mk --table <project-id>:streamsight_data.event_counts \
  window_start:TIMESTAMP,window_end:TIMESTAMP,event_type:STRING,event_count:INTEGER

gcloud storage buckets create gs://<your-bucket-name> --location=<your-region>
```

### 3. Run locally (no cloud cost)
```bash
python publisher.py            # Terminal 1 — generates fake events
python streaming_pipeline.py   # Terminal 2 — processes events, writes to BigQuery
```

### 4. Deploy to Dataflow (cloud)
Update `streaming_pipeline.py` `PipelineOptions` with `runner="DataflowRunner"`, your project ID, region, and GCS bucket paths, then run:
```bash
python streaming_pipeline.py
```
**Important:** Cancel the Dataflow job when done testing to avoid ongoing charges:
```bash
gcloud dataflow jobs list --region=<your-region>
gcloud dataflow jobs cancel <JOB_ID> --region=<your-region>
```

### 5. View the dashboard
Connect a Looker Studio report to the `streamsight_data.event_counts` BigQuery table. See screenshots below.

## Dashboard Preview

_Add a screenshot of your Looker Studio dashboard here._

```
![Dashboard](dashboard.png)
```

## Key Design Decisions

- **Fixed windows (10s) with early triggers**: balances near-real-time visibility with manageable write volume to BigQuery, rather than writing on every single event.
- **`AccumulationMode.DISCARDING`**: each trigger fire emits only new counts since the last firing, avoiding duplicate/cumulative totals.
- **Autoscaling Dataflow workers (1–100)**: keeps cost low during idle periods and scales up automatically under load.
- **Region selection**: Pub/Sub and BigQuery resources are hosted in the lowest-latency region for the primary user base; Dataflow worker region can be adjusted independently for capacity availability.

## Lessons Learned

- Fixed windows align to absolute epoch time, not pipeline start time — a subtle behavior that affects how test data must be timestamped to validate aggregation logic.
- Streaming Dataflow jobs bill continuously and must be explicitly cancelled; local `DirectRunner` testing is the safer/cheaper way to iterate before cloud deployment.
- Regional compute capacity can be temporarily exhausted on smaller cloud regions; a fallback region (e.g., `us-central1`) is useful for reliability.

## Possible Future Improvements

- Partition the BigQuery table by `window_start` for cost efficiency at scale
- Add anomaly detection (statistical thresholding + Isolation Forest) on top of windowed aggregates
- Add integration tests for the pipeline transforms
- Parameterize region/project/bucket via environment variables or a config file

## Author

Built as a self-directed learning project to understand real-time streaming architecture on Google Cloud.
