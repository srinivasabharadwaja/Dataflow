import apache_beam as beam
import argparse
import logging
import csv, os
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "training-creds.json"


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/trainingproject-317506/subscriptions/training-sub'
                        )
    parser.add_argument('--inputBucket',
                        dest='inputBucket',
                        help='File to read',
                        default='gs://bharadwaja-gcp-training/demo_data.txt'
                        )
    parser.add_argument('--output_table',
                        dest='output_table',
                        help='--output_table_name',
                        default='dataflow.pub_gcs_bq'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        p1 = p | "Read from Bucket">>beam.io.ReadFromText(known_args.inputBucket)
        p2 = p | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription)
        merged = (p1, p2) | "Merge two" >> beam.Flatten() 
        
        pubsub_data = (
                    merged
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    | 'Split Row' >> beam.Map(lambda row : row.split(','))
                    | beam.Map(lambda x: {"Store_id": x[0], "sum": x[1]}) 
                    | 'Write to BigQuery' >> beam.io.WriteToBigQuery(known_args.output_table,
                    schema='Store_id:STRING, sum:STRING', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                    )
        
        p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()