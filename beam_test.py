import apache_beam as beam

# A tiny static list, pretending to be "events"
sample_events = [
    "click", "click", "purchase", "signup", "click", "logout", "purchase"
]

with beam.Pipeline() as pipeline:
    (
        pipeline
        | "Create events" >> beam.Create(sample_events)
        | "Pair with 1" >> beam.Map(lambda event: (event, 1))
        | "Count per type" >> beam.CombinePerKey(sum)
        | "Print results" >> beam.Map(print)
    )