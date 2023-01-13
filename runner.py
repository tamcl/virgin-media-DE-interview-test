import apache_beam as beam
from datetime import datetime
from dateutil import parser
import pytz
import csv

import datetime as datetime
import pandas as pd
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner

from apache_beam.options.pipeline_options import PipelineOptions

TABLE_ORDER = ["timestamp", "origin", "destination", "transaction_amount"]
TABLE_SCHEMA = {"timestamp": parser.parse, "origin": str, "destination": str, "transaction_amount": float}
utc = pytz.UTC


def read_csv_file(row):
    # print(type(row))
    # print(row)
    elements = row.split(',')
    elements_dict = {}
    for i in range(len(elements)):
        elements_dict[TABLE_ORDER[i]] = TABLE_SCHEMA[TABLE_ORDER[i]](elements[i])
    return elements_dict


def over_20(element):
    return element["transaction_amount"] > 20


def not_before_2010(element):
    return not element["timestamp"] < datetime.datetime(2010, 1, 1).replace(tzinfo=utc)


def datetime_format(element):
    return datetime.strptime(element, "%Y-%m-%d %H:%M:%S UTC")


with beam.Pipeline() as p:
    elements = (p | beam.io.ReadFromText(
        'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
                | beam.Map(read_csv_file)
                | beam.Filter(over_20)
                | beam.Filter(not_before_2010)
                | beam.io.WriteToText('./output/test.csv'))
#

# p = beam.Pipeline(InteractiveRunner())
# rows = (
#     p
#     | beam.io.filesystems.FileSystems.open('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv')  # emits FileMetadatas
#     | beam.FlatMap(read_csv_file))                          # emits rows
#     pass
# beam_options = PipelineOptions()
#
#
# class MyOptions(PipelineOptions):
#     @classmethod
#     def _add_argparse_args(cls, parser):
#         parser.add_argument(
#             '--input',
#             default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
#             help='The file path for the input text to process.')
#         parser.add_argument(
#             '--output', default='./output/', help='The path prefix for output files.')
