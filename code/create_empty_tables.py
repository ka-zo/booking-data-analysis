#!/usr/bin/env python3
# *_* coding: utf-8 *_*

"""Create empty airports and bookings table in Google BigQuery

Don't forget to perform authentication before, following the tutorial here:
https://cloud.google.com/bigquery/docs/authentication/getting-started 
"""

__author__ = "Zoltán Katona, PhD"
__copyright__ = f"Copyright 2024, {__author__}"
__license__ = "BSD-3-Clause"
__version__ = "0.1.0"

import argparse
import logging
import sys

from google.cloud import bigquery

def create_empty_airports_table(table_id:str):
    """Create empty airports table

    Args:
        table_id (str): table to be created, format:
                        "<project_id>.<dataset_id>.<table_name>"
                        format shall be checked by BigQuery
    """

    # Construct a BigQuery client object.
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("airport_id", "INTEGER", mode="REQUIRED",
                            description="Unique OpenFlights identifier for "\
                                "this airport."),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED",
                            description="Name of airport. May or may not "\
                                "contain the City name."),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED",
                            description="Main city served by airport. May "\
                                "be spelled differently from Name."),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED",
                            description="Country or territory where airport "\
                                "is located."),
        bigquery.SchemaField("iata", "STRING", mode="NULLABLE",
                            max_length=3,
                            description="3-letter IATA code. Null if not "\
                                "assigned/unknown."),
        bigquery.SchemaField("icao", "STRING", mode="NULLABLE",
                            max_length=4,
                            description="4-letter ICAO code. Null if not "\
                                "assigned."),
        bigquery.SchemaField("latitude", "NUMERIC", mode="REQUIRED",
                            description="Decimal degrees, usually to six "\
                                "significant digits. Negative is South, "\
                                "positive is North."),
        bigquery.SchemaField("longitude", "NUMERIC", mode="REQUIRED",
                            description="Decimal degrees, usually to six "\
                                "significant digits. Negative is West, "\
                                "positive is East."),
        bigquery.SchemaField("altitude", "NUMERIC", mode="REQUIRED",
                            description="In feet."),
        bigquery.SchemaField("timezone_hours", "NUMERIC", mode="NULLABLE",
                            description="Hours offset from UTC. Fractional "\
                                "hours are expressed as decimals, eg. India "\
                                "is 5.5."),
        bigquery.SchemaField("dst", "STRING", mode="NULLABLE",
                            max_length=1,
                            description="Daylight savings time. One of E "\
                                "(Europe), A (US/Canada), S (South America), "\
                                "O (Australia), Z (New Zealand), N (None) or "\
                                "U (Unknown)."),
        bigquery.SchemaField("timezone_string", "STRING", mode="NULLABLE",
                            description="Timezone in 'tz' (Olson) format, "\
                                "eg. 'America/Los_Angeles'."),
        bigquery.SchemaField("type", "STRING", mode="REQUIRED",
                            description="Type of the airport. Value "\
                                "'airport' for air terminals, 'station' for "\
                                "train stations, 'port' for ferry terminals "\
                                "and 'unknown' if not known. In "\
                                "airports.csv, only type=airport is "\
                                "included."),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE",
                            description="Source of this data. 'OurAirports' "\
                                "for data sourced from OurAirports, 'Legacy' "\
                                "for old data not matched to OurAirports "\
                                "(mostly DAFIF), 'User' for unverified user "\
                                "contributions. In airports.csv, only "\
                                "source=OurAirports is included."),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        table = client.create_table(table)  # Make an API request.
        logging.info(
            "Created table %s.%s.%s", table.project, table.dataset_id, table.table_id
        )
    except Exception as err:
        logging.exception("Unexpected error happened: %s, %s", err, type(err))
        sys.exit(1)

def create_empty_bookings_table(table_id:str):
    """Create empty bookings table

    Args:
        table_id (str): table to be created, format:
                        "<project_id>.<dataset_id>.<table_name>"
                        format shall be checked by BigQuery
    """

    # Construct a BigQuery client object.
    client = bigquery.Client()

    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED",
                            description="When the event was created"),
        bigquery.SchemaField("uci", "STRING", mode="REQUIRED",
                            description="The unique identifier of the "\
                            "passenger"),
        bigquery.SchemaField("age", "INTEGER", mode="NULLABLE",
                            description="The age of the passenger"),
        bigquery.SchemaField("passenger_type", "STRING", mode="NULLABLE",
                            description="The type of the passenger, an "\
                            "enum with the following possible values: "\
                            "[Adt, Chd]"),
        bigquery.SchemaField("booking_status", "STRING", mode="REQUIRED",
                            description="The status of the booking, an "\
                            "enum with the following possible values: "\
                            "[Confirmed, Cancelled, WaitingList, OnRequest, "\
                            "SeatAvailable, Unaccepted]"),
        bigquery.SchemaField("operating_airline", "STRING", mode="REQUIRED",
                            max_length=2,
                            description="The 2 character code of the airline "\
                            "operating the flight, KL is KLM"),
        bigquery.SchemaField("origin_airport", "STRING", mode="NULLABLE",
                            max_length=3,
                            description="The IATA code of the departure "\
                            "airport"),
        bigquery.SchemaField("destination_airport", "STRING", mode="NULLABLE",
                            max_length=3,
                            description="The IATA code of the destination "\
                            "airport"),
        bigquery.SchemaField("departure_date", "TIMESTAMP", mode="NULLABLE",
                            description="The UTC time of the flight "\
                            "departure"),
        bigquery.SchemaField("arrival_date", "TIMESTAMP", mode="NULLABLE",
                            description="The UTC time of the flight arrival"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        table = client.create_table(table)  # Make an API request.
        logging.info(
            "Created table %s.%s.%s", table.project, table.dataset_id, table.table_id
        )
    except Exception as err:
        logging.exception("Unexpected error happened: %s, %s", err, type(err))
        sys.exit(1)


def parse_command_line() -> list[str]:
    """Parse command line arguments

    Returns:
        table_ids (list[str]):  list of table ids to be created in the format
                                "<project_id>.<dataset_id>.<table_name>"
                                format shall be checked by BigQuery.
                                table_id[0] is the bookings table id
                                table_id[1] is the airports table id
    """

    parser = argparse.ArgumentParser(
        prog="create_empty_tables",
        description="Create empty airports and bookings table in BigQuery. "\
            "Don't forget to perform authentication before, using gcloud "\
            "init.",
        epilog="Thanks for using %(prog)s! :)",
    )
    parser.add_argument("-b", "--bookings_table_id",
                        help="BigQuery table to be created, format: "\
                            "<project_id>.<dataset_id>.<table_name>, "\
                            "default: booking-data-analysis."\
                            "booking_data_analysis.bookings",
                        default="booking-data-analysis."\
                            "booking_data_analysis.bookings")
    parser.add_argument("-a", "--airports_table_id",
                        help="BigQuery table to be created, format: "\
                            "<project_id>.<dataset_id>.<table_name>, "\
                            "default: booking-data-analysis."\
                            "booking_data_analysis.airports",
                        default="booking-data-analysis."\
                            "booking_data_analysis.airports")

    args = parser.parse_args()
    bookings_table_id:str = args.bookings_table_id
    airports_table_id:str = args.airports_table_id

    return [bookings_table_id, airports_table_id]

def main():
    """Main entry point for the code."""

    logging.basicConfig(
        encoding='utf-8',
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S (%z)',
    )

    bookings_table_id, airports_table_id = parse_command_line()

    create_empty_bookings_table(bookings_table_id)
    create_empty_airports_table(airports_table_id)

if __name__ == "__main__":
    main()
