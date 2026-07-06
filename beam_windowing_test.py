import apache_beam as beam
from apache_beam import window

# Fixed base timestamp (epoch seconds) - deliberately a clean number
BASE_TIME = 1700000000  # a fixed reference point, not "now"

sample_events = [
    ("click", 0),
    ("click", 1),      # same 2-sec window as above (0-2)
    ("purchase", 2),
    ("signup", 3),      # same window as purchase (2-4)
    ("click", 4),
    ("logout", 5),
    ("purchase", 5.5),  # same window as click/logout (4-6)
]

class AddTimestamp(beam.DoFn):
    def process(self, element):
        event_type, offset_seconds = element
        yield beam.window.TimestampedValue(event_type, BASE_TIME + offset_seconds)

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Create events" >> beam.Create(sample_events)
        | "Add timestamps" >> beam.ParDo(AddTimestamp())
        | "Window into 2-sec buckets" >> beam.WindowInto(window.FixedWindows(2))
        | "Pair with 1" >> beam.Map(lambda event: (event, 1))
        | "Count per window" >> beam.CombinePerKey(sum)
        | "Print results" >> beam.Map(print)
    )