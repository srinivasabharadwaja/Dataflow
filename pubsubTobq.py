import apache_beam as beam
import argparse
import logging
import csv
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount

# credential_path = "training-gcp.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "training-creds.json"


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/trainingproject-317506/subscriptions/training-sub'
                        )
    parser.add_argument('--output_table',
                        dest='output_table',
                        help='--output_table_name',
                        default='dataflow.table2'
                        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        pubsub_data = (
                    p 
                    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= known_args.inputSubscription)
                    | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))
                    | 'Split Row' >> beam.Map(lambda row : row.decode().split(','))
                    |beam.Map(lambda x: {"Store_id": x[0], "sum": x[1]}) 
                    | 'Write to pus sub' >> beam.io.WriteToBigQuery(known_args.output_table,
                    #schema=' Store_id:STRING, Store_location:STRING, Product_id:STRING , Product_category:INTEGER ,number_of_pieces_sold:INTEGER , buy_rate:INTEGER,sell_price:INTEGER,unix_timestamp:INTEGER',
                    schema=' Store_id:STRING, sum:STRING', 
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                )
        
        p.run().wait_until_finish()
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

