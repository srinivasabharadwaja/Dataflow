
import logging
import re
import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from google.cloud import pubsub_v1
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
#credential_path = "training-gcp.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "training-creds.json"


#python gcs_bq.py --runner DirectRunner --project trainingproject-317506 --region us-central1 --temp_location gs://bharadwaja-gcp-training/dataflow/temp --staging_location gs://bharadwaja-gcp-training/dataflow/staging



def run(argv=None, save_main_session=True):
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | 'Read' >> ReadFromText('gs://bharadwaja-gcp-training/demo_data.txt')

    counts = (
        lines
        |'split'  >> beam.Map(lambda record: record.split(','))
        | beam.Map(lambda x: {"id": x[0], "name": x[1]}) 
        | 'Write to BigQuery' >> beam.io.WriteToBigQuery('dataflow.test1',
                    schema=' id:STRING, name:STRING', 

                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
                )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


