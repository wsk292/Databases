import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# DoFn to perform on each element in the input PCollection.
class OpenCloseRangeFn(beam.DoFn):
  def process(self, element):
    record = element
    ID = record.get('ID')
    Symbol = record.get('Stock')
    date = record.get('Date')
    Open = record.get('Open')
    High = record.get('High')
    Low = record.get('Low')
    Close = record.get('Close')
    Adj_Close = record.get('Adj_Close')
    Volume = record.get('Volume')

    # returns range from open to close
    return [(ID, int(Close) - int(Open))]

# DoFn performs on each element in the input PCollection.
class MakeRecordFn(beam.DoFn):
  def process(self, element):
     ID, Range = element
     record = {'ID': ID, 'Range': Range}
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

    # reads in data from the ETH BigQuery Table, limit of 1000
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(
        query = 'SELECT * FROM econ_data.Amazon LIMIT 1000')
        )

    print(query_results)
    # write PCollection to log file
    query_results | 'Write to input file' >> WriteToText('input.txt')

    # Extract the range
    range_pcoll = query_results | 'Extract Range' >> beam.ParDo(OpenCloseRangeFn())

    # write PCollection to log file
    range_pcoll | 'Write to log 1' >> WriteToText('Amazon.txt')

    # write PCollection to a file
    range_pcoll | 'Write File' >> WriteToText('output.txt')

    # make BQ records
    out_pcoll = range_pcoll | 'Make BQ Record' >> beam.ParDo(MakeRecordFn())

    # sets new table name and schema
    qualified_table_name = PROJECT_ID + ':econ_data.Amazon_range'
    table_schema = 'ID:INTEGER,Range:INTEGER'

    # writes the table to BigQuery
    out_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name,
                                                    schema=table_schema,
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
