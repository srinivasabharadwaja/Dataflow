import logging
import re
import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from google.cloud import pubsub_v1
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
# credential_path = "training-gcp.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "training-creds.json"

class SendAsBatch(beam.DoFn):
  
  def process(self, element):
    publisher = pubsub_v1.PublisherClient()
    topic_id = "projects/trainingproject-317506/topics/training"
    data=element
    data = data.encode("utf-8")
    future = publisher.publish(topic_id, data)

    print(future.result())



def run(argv=None, save_main_session=True):
  pipeline_options = PipelineOptions()
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | 'Read' >> ReadFromText('gs://bharadwaja-gcp-training/demo_data.txt')

    counts = (
        lines
        | 'To pubsub' >> (beam.ParDo(SendAsBatch()).with_output_types(str)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


