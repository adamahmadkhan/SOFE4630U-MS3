import argparse
import json
import apache_beam as beam

from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions
)


def parse_json(message):
    """Convert Pub/Sub bytes into a Python dictionary."""
    return json.loads(message.decode("utf-8"))


def valid_measurement(record):
    """Remove records where any measurement is missing."""
    return (
        record.get("temperature") is not None
        and record.get("humidity") is not None
        and record.get("pressure") is not None
    )


def convert_units(record):
    """Convert temperature C to F and pressure kPa to psi."""
    record = dict(record)

    record["temperature"] = record["temperature"] * 1.8 + 32
    record["pressure"] = record["pressure"] / 6.895

    return record


def encode_json(record):
    """Convert dictionary back into bytes for Pub/Sub."""
    return json.dumps(record).encode("utf-8")


def run():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_topic",
        required=True,
        help="Input Pub/Sub topic"
    )

    parser.add_argument(
        "--output_topic",
        required=True,
        help="Output Pub/Sub topic"
    )

    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)

    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline

            | "Read Smart Meter Readings"
            >> beam.io.ReadFromPubSub(
                topic=known_args.input_topic
            )

            | "Parse JSON"
            >> beam.Map(parse_json)

            | "Filter Missing Measurements"
            >> beam.Filter(valid_measurement)

            | "Convert Units"
            >> beam.Map(convert_units)

            | "Encode JSON"
            >> beam.Map(encode_json)

            | "Write Processed Readings"
            >> beam.io.WriteToPubSub(
                topic=known_args.output_topic
            )
        )


if __name__ == "__main__":
    run()
