import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
import json

PROJECT_ID = "streamsight-project"
TOPIC_PATH = f"projects/{PROJECT_ID}/topics/streamsight-events"
TABLE_SPEC = f"{PROJECT_ID}:streamsight_data.event_counts"

TABLE_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_count", "type": "INTEGER", "mode": "NULLABLE"},
    ]
}

def parse_event(message_bytes):
    try:
        event = json.loads(message_bytes.decode("utf-8"))
        return event.get("event_type", "unknown")
    except Exception:
        return "unparseable"

class FormatForBigQuery(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, count = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
            "event_type": event_type,
            "event_count": count,
        }


options = PipelineOptions(
    runner="DataflowRunner",
    project=PROJECT_ID,
    region="us-central1",
    temp_location="gs://streamsight-project-dataflow/temp",
    staging_location="gs://streamsight-project-dataflow/staging",
    job_name="streamsight-pipeline-v2",
)
options.view_as(StandardOptions).streaming = True


with beam.Pipeline(options=options) as pipeline:
    (
        pipeline
        | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=TOPIC_PATH)
        | "Parse event type" >> beam.Map(parse_event)
        | "Window into 10-sec buckets" >> beam.WindowInto(
              window.FixedWindows(10),
              trigger=AfterWatermark(early=AfterProcessingTime(5)),
              accumulation_mode=AccumulationMode.DISCARDING
          )
        | "Pair with 1" >> beam.Map(lambda event: (event, 1))
        | "Count per window" >> beam.CombinePerKey(sum)
        | "Format for BigQuery" >> beam.ParDo(FormatForBigQuery())
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
              TABLE_SPEC,
              schema=TABLE_SCHEMA,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
              create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
          )
    )