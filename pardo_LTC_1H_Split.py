import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class OpenCloseRangeFn(beam.DoFn):
  def process(self, element):
    record = element
    ID = record.get('id')
    Symbol = record.get('Symbol')
    date = record.get('date')
    time = record.get('time')
    Open = record.get('Open')
    High = record.get('High')
    Low = record.get('Low')
    Close = record.get('Close')
    Volume_LTC = record.get('Volume_LTC')
    Volume_USD = record.get('Volume_USD')

    return [(ID, int(Close) - int(Open))]

# DoFn performs on each element in the input PCollection.
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     ID, Range = element
     record = {'id': ID, 'range': Range}
     return [record] 
    

PROJECT_ID = os.environ['PROJECT_ID']
print(PROJECT_ID)

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:

    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(
        query = 'SELECT * FROM crypto_market_data_transformed.LTC_1H_Split LIMIT 1000')
        )

    print(query_results)
    # write PCollection to log file
    query_results | 'Write to input file' >> WriteToText('input.txt')

    # Extract the range
    range_pcoll = query_results | 'Extract Range' >> beam.ParDo(OpenCloseRangeFn())

    # write PCollection to log file
    range_pcoll | 'Write to log 1' >> WriteToText('LTC_ID_range.txt')

    # write PCollection to a file
    range_pcoll | 'Write File' >> WriteToText('output.txt')
    
    # make BQ records
    out_pcoll = range_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())
    
    qualified_table_name = PROJECT_ID + ':crypto_market_data_transformed.LTC_ID_range'
    table_schema = 'id:INTEGER,range:INTEGER'
    
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
