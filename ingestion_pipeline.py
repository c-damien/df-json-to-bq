import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

#parse line by line the JSONL content
class ExtractFieldsFn(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element)
            encounter_id = data.get('encounter_id')
            user_id = data.get('user_id')
            yield {
                'data': json.dumps(data), #the whole JSON object
                'encounter_id': encounter_id, #extracted from the JSON object as fields
                'user_id': user_id
            }
        except Exception as e:
            logging.error(f"Failed to parse JSON line: {e}")


# main
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', help='GCS bucket name (ignored if --custom_file_pattern is provided)')
    parser.add_argument('--custom_file_pattern', help='Path to a local file or explicit GCS file pattern to read from directly')
    parser.add_argument('--file_pattern', default='*/*/*/deid_object_masked.json', help='GCS glob pattern relative to the bucket (default: */*/*/deid_object_masked.json)')
    parser.add_argument('--project', required=True, help='GCP Project ID')
    parser.add_argument('--dataset', required=True, help='BigQuery Dataset ID')
    parser.add_argument('--table', required=True, help='BigQuery Table ID')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    if not known_args.bucket and not known_args.custom_file_pattern:
        parser.error('Either --bucket or --custom_file_pattern must be provided')
        
    if known_args.custom_file_pattern: #use this parameter to specify the full path yourself
        input_pattern = known_args.custom_file_pattern
    else:
        # Clean up bucket name if gs:// or trailing slash is passed
        bucket = known_args.bucket.strip()
        if bucket.startswith("gs://"):
            bucket = bucket[5:]
        bucket = bucket.rstrip("/")
        # Clean up file pattern if leading slash is passed
        file_pattern = known_args.file_pattern.strip().lstrip("/")
        # force GCS pattern: bucket/file_pattern
        input_pattern = f"gs://{bucket}/{file_pattern}"
        
    output_table = f"{known_args.project}:{known_args.dataset}.{known_args.table}"
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    
    # Define the BigQuery schema
    schema = {
        'fields': [
            {'name': 'data', 'type': 'JSON', 'mode': 'NULLABLE'},
            {'name': 'encounter_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }
    
    logging.info(f"Reading from pattern: {input_pattern}")
    logging.info(f"Writing to table: {output_table}")
    

    ### here is our pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'ReadFiles' >> beam.io.ReadFromText(input_pattern)
         | 'ExtractFields' >> beam.ParDo(ExtractFieldsFn())
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             output_table,
             schema=schema,
             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         ))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
