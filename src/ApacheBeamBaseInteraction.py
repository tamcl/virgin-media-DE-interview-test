import datetime


import apache_beam as beam
import pandas as pd
import pytz
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.testing.util import assert_that, equal_to
from dateutil import parser

TABLE_ORDER = ["timestamp", "origin", "destination", "transaction_amount"]
TABLE_SCHEMA = {
    "timestamp": parser.parse,
    "origin": str,
    "destination": str,
    "transaction_amount": float,
}
EXPECTED_OUTPUT = [
    ("2017-03-18", 2102.22),
    ("2017-08-31", 13700000023.08),
    ("2018-02-27", 129.12),
]


# ## Task 1
# Write an Apache Beam batch job in Python satisfying the following requirements
# 1. Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
# 1. Find all transactions have a `transaction_amount` greater than `20`
# 1. Exclude all transactions made before the year `2010`
# 1. Sum the total by `date`
# 1. Save the output into `output/results.jsonl.gz` and make sure all files in the `output/` directory is git ignored

# If the output is in a CSV file, it would have the following format
# ```
# date, total_amount
# 2011-01-01, 12345.00
# ...
# ```


class ApacheBeamBaseInteraction:
    def __init__(
        self, input_url_src: str, local_output_dest: str,
    ):
        self.input_url_src = input_url_src
        self.local_output_dest = local_output_dest


class VirginMediaTestOne(ApacheBeamBaseInteraction):
    def __init__(
        self,
        input_url_src="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
        local_output_dest="./output/test.csv",
    ):
        super().__init__(
            input_url_src=input_url_src, local_output_dest=local_output_dest,
        )

    def process(self):
        with beam.Pipeline() as p:
            # read file as csv
            transactions_df = p | "Read CSV" >> beam.dataframe.io.read_csv(
                self.input_url_src, parse_dates=["timestamp"]
            )
            # convert into pcollection
            transactions_collection = beam.dataframe.convert.to_pcollection(
                transactions_df
            )
            #filter out transaction amount
            large_transaction_collection = (
                transactions_collection
                | "Get large transaction"
                >> beam.Filter(self.find_all_transaction_amount_over_20)
            )
            #filter out timestamp
            new_large_transaction_collection = (
                large_transaction_collection
                | "Remove old transaction" >> beam.Filter(self.exclude_all_before_2010)
            )
            #convert into df
            new_large_transaction_df = beam.dataframe.convert.to_dataframe(
                new_large_transaction_collection
            )
            # group transaction amount by date
            new_large_transaction_df["date"] = new_large_transaction_df[
                "timestamp"
            ].apply(lambda x: x.date())
            transaction_per_day_series = new_large_transaction_df.transaction_amount.groupby(
                new_large_transaction_df["date"]
            ).sum()
            transaction_per_day_series.name = "total_amount"

            #output to csv
            transaction_per_day_series.to_csv(self.local_output_dest)

    @staticmethod
    def find_all_transaction_amount_over_20(element):
        """
        return all rows that has a transaction amount > 20
        :param element:
        :return:
        """
        return element.transaction_amount > 20

    @staticmethod
    def exclude_all_before_2010(element):
        """
        return all rows that does not have timestamp before 2010
        :param element:
        :return:
        """

        return not element.timestamp < pd.Timestamp(
            datetime.datetime(2010, 1, 1).replace(tzinfo=pytz.UTC)
        )


# ## Task 2
# Following up on the same Apache Beam batch job, also do the following
# 1. Group all transform steps into a single `Composite Transform`
# 1. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam


class VirginMediaTestTwo(VirginMediaTestOne):
    def __init__(
        self,
        input_url_src="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
        local_output_dest="./output/test2.csv",
    ):
        super().__init__(
            input_url_src=input_url_src, local_output_dest=local_output_dest,
        )

    def process(self):
        with beam.Pipeline() as p:
            # Composite transform
            output = (
                p
                | beam.io.ReadFromText(self.input_url_src, skip_header_lines=1)
                | CsvToDict() # convert csv to dictionary
                | FilterAmount() #filter rows by transaction amount
                | FilterDate() #filter rows by date
                | SumByDate() # group by date
            )

            #export output to desired destination
            output | beam.Map(lambda x: f"{x[0]}, {x[1]}") | beam.io.WriteToText(
                self.local_output_dest
            )

            # Unit test
            assert_that(output, equal_to(EXPECTED_OUTPUT))


class CsvToDict(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Map(lambda line: dict(zip(TABLE_ORDER, line.split(","))))


class FilterAmount(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Filter(lambda x: float(x["transaction_amount"]) > 20)


class FilterDate(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.Filter(
            lambda x: datetime.datetime.strptime(
                x["timestamp"], "%Y-%m-%d %H:%M:%S UTC"
            ).date()
            >= datetime.datetime(2010, 1, 1).date()
        )


class SumByDate(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | beam.Map(
                lambda x: (
                    datetime.datetime.strptime(
                        x["timestamp"], "%Y-%m-%d %H:%M:%S UTC"
                    ).strftime("%Y-%m-%d"),
                    float(x["transaction_amount"]),
                )
            )
            | beam.GroupByKey()
            | beam.Map(lambda x: (x[0], sum(x[1])))
        )
