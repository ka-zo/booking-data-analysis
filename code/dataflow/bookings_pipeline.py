#!/usr/bin/env python3
# *_* coding: utf-8 *_*

"""This module performs ETL of flight bookings data into BigQuery.

    Example command line execution:

    Output to BigQuery:
        python code/dataflow/bookings_pipeline.py --big_query \
        -b data/bookings/booking.json --temp_location gs://bookings-temp

    Output to local file:
        python code/dataflow/bookings_pipeline.py -b data/bookings/booking.json
"""

__author__ = "Zoltán Katona, PhD"
__copyright__ = f"Copyright 2024, {__author__}"
__license__ = "BSD-3-Clause"
__version__ = "0.1.0"

import argparse
from datetime import datetime
import itertools
import json
import logging
from pathlib import Path
import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
import apache_beam.io.gcp.bigquery as bq

class JSON2CleanTuple(beam.DoFn):
    """Convert relevant fields of booking from json format to tuple[dict]"""

    # pylint: disable=W0223
    # pylint: disable=R0903

    PROPER_BOOKING = "proper_booking"
    INCORRECT_BOOKING = "incorrect_booking"

    class Output:
        """Helper class to collect data processing output"""
        def __init__(self, result=None, error=None):
            self.result:tuple[dict] = result
            self.error:str = error

    def process(self, element:str):
        # pylint: disable=W0221
        """Process json strings

        Args:
            element (str): A single json string containing bookings

        Returns:
            TaggedOutput: value for tags
                - proper_booking -      tuple of the cross product of passenger
                                        and flight dictionaries
                - incorrect_booking -   input json string and error message

        """
        output:JSON2CleanTuple.Output = JSON2CleanTuple.json2tuple(element)
        if output.result:
            yield beam.pvalue.TaggedOutput(
                    JSON2CleanTuple.PROPER_BOOKING,
                        tuple(output.result)
                )
        else:
            yield beam.pvalue.TaggedOutput(
                    JSON2CleanTuple.INCORRECT_BOOKING,
                    {
                        "json": str(element),
                        "error": str(output.error),
                    }
            )

    class InvalidTimestampException(Exception):
        """Custom exception to signal invalid timestamp format"""

    class InvalidAgeException(Exception):
        """Custom exception to signal invalid age of a person"""

    class InvalidPassengerTypeException(Exception):
        """Custom exception to signal invalid passenger type"""

    class InvalidBookingStatusException(Exception):
        """Custom exception to signal invalid booking status"""

    class InvalidOperatingAirlineException(Exception):
        """Custom exception to signal invalid operating airline"""

    class InvalidIATACodeException(Exception):
        """Custom exception to signal invalid IATA code"""

    @staticmethod
    def check_timestamp(timestamp:str):
        """Check if timestamp is one of the expected formats

        Args:
            timestamp (str): timestamp with date and time

        Raises:
            JSON2Tuple.InvalidTimestampException: exception if timestamp
            format is not known
        """

        exception = ''
        for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']:
            try:
                datetime.strptime(timestamp, fmt)
                return
            # pylint: disable=broad-exception-caught
            except Exception as e:
                exception = e

        raise JSON2CleanTuple.InvalidTimestampException(f"Timestamp '{timestamp}' "\
                                        "does not match any of "\
                                        "['%Y-%m-%dT%H:%M:%S.%fZ', "\
                                        "'%Y-%m-%dT%H:%M:%SZ']") from exception

    @staticmethod
    def check_age(age:int):
        """Check if age is meaningful

        Args:
            age (int): age of a person

        Raises:
            JSON2Tuple.InvalidAgeException: exception if age is invalid
        """

        try:
            if age < 0 | age > 150:
                raise ValueError(f"Age '{age}' should be: 0 <= age <= 150")
        except Exception as e:
            raise JSON2CleanTuple.InvalidAgeException from e

    @staticmethod
    def check_passenger_type(passenger_type:str):
        """Check if passenger type is one of the supported values
        ['Adt', 'Chd']

        Args:
            passenger_type (str): type of the passenger

        Raises:
            JSON2Tuple.InvalidPassengerTypeException: exception if
            passenger type is not supported
        """

        try:
            if passenger_type not in ['adt', 'chd']:
                raise ValueError(f"passengerType '{passenger_type}'is not "\
                                 "one of [Adt, Chd]")
        except Exception as e:
            raise JSON2CleanTuple.InvalidPassengerTypeException from e

    @staticmethod
    def check_booking_status(booking_status:str):
        """Check if booking status is one of the supported values
        ['CONFIRMED', 'CANCELLED', 'WAITING_LIST',
        'ON_REQUEST', 'SEAT_AVAILABLE', 'UNACCEPTED']

        Args:
            booking_status (str): status of the booking

        Raises:
            JSON2Tuple.InvalidBookingStatusException: exception if
            booking status is not supported
        """

        try:
            if booking_status not in \
            ['confirmed', 'cancelled', 'waiting_list',
            'on_request', 'seat_available', 'unaccepted']:
                raise ValueError(f"bookingStatus '{booking_status}' is not "\
                                 "one of ['Confirmed', 'Cancelled', "\
                                "'WaitingList', 'OnRequest', "\
                                "'SeatAvailable', 'Unaccepted']")
        except Exception as e:
            raise JSON2CleanTuple.InvalidBookingStatusException from e

    @staticmethod
    def check_operating_airline(operating_airline:str):
        """Check if operating airline string is 2 characters long

        Args:
            operating_airline (str): operating airline identifier

        Raises:
            JSON2Tuple.InvalidOperatingAirlineException: exception if
            operating airline identifier does not have the correct length
        """

        try:
            if len(operating_airline) != 2:
                raise ValueError(f"operatingAirline '{operating_airline}' is "\
                                 " not 2 characters long")
        except Exception as e:
            raise JSON2CleanTuple.InvalidOperatingAirlineException from e

    @staticmethod
    def check_iata_code(iata_code:str):
        """Check if IATA code string length is 3

        Args:
            iata_code (str): IATA code

        Raises:
            JSON2CleanTuple.InvalidIATACodeException: exception if
            IATA code does not have the correct length
        """

        try:
            if len(iata_code) != 3:
                raise ValueError(f"IATA code '{iata_code}' is not 3 "\
                                 "characters long")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise JSON2CleanTuple.InvalidIATACodeException from e

    @staticmethod
    def json2tuple(bookings_json:str) -> Output:
        """Convert a json string of bookings into a cross-product of
        passengers and flights. There will be an output row for each
        flight of each person.

        When the complete json object is invalid, the data shall be
        ignored, the event shall be logged as an exception for later
        inspection.

        When the timestamp of a booking json object is missing, or there are
        missing or empty passenger or product data, the complete json object
        shall be ignored, and the event shall be logged as an exception for
        later inspection.

        When a required field is missing, the corresponding row for the flight
        of that person shall be ignored from the output, but logged as an
        exception for later inspection.

        When a nullable field is missing, the event shall be logged as a
        warning for later inspection.
        
        When a nullable field has invalid data, the event shall be logged as
        warning for later inspection, and the invalid value of the field shall
        be replaced with None in the output.

        Args:
            bookings_json (str): json object representing bookings

        Returns:
            Output: class containing either the cross-product of passengers
                    and flights or an error message
        """
        # pylint: disable=too-many-return-statements, too-many-branches,
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals

        try:
            bookings = json.loads(bookings_json)
        # pylint: disable=broad-exception-caught
        except Exception as e:
            logging.exception("Invalid json: '%s': %s - %s",
                                bookings_json, e, type(e))
            return JSON2CleanTuple.Output(error='Invalid json')

        try:
            timestamp = bookings['timestamp']
            JSON2CleanTuple.check_timestamp(timestamp)
        except JSON2CleanTuple.InvalidTimestampException as e:
            logging.exception("Invalid timestamp in booking '%s': %s - %s",
                              bookings_json, e, type(e))
            return JSON2CleanTuple.Output(error='Invalid timestamp')
        except KeyError as e:
            logging.exception("Missing timestamp in booking: "\
                                "%s - %s", e, bookings_json)
            return JSON2CleanTuple.Output(error='Missing timestamp')

        try:
            passengers = bookings['event']['DataElement']\
                ['travelrecord']['passengersList']
            if not passengers:
                raise ValueError('List of passengers is empty')
        # pylint: disable=broad-exception-caught
        except Exception as e:
            logging.exception("Problem with the list of passengers in "\
                              "booking '%s': %s - %s",
                              bookings_json, e, type(e))
            return JSON2CleanTuple.Output(error='Missing or empty list of passengers')

        try:
            products = bookings['event']['DataElement']['travelrecord']['productsList']
            if not products:
                raise ValueError('List of products is empty')
        # pylint: disable=broad-exception-caught
        except Exception as e:
            logging.exception("Problem with the list of products in "\
                              "booking '%s': %s - %s",
                              bookings_json, e, type(e))
            return JSON2CleanTuple.Output(error='Missing or empty list of products')

        passenger_entries = []
        for passenger in passengers:
            uci = None
            age = None
            passenger_type = None
            try:
                uci = passenger['uci']
            except KeyError as e:
                logging.exception("Missing uci in passenger data: "\
                                    "%s - %s", e, bookings_json)
                continue

            try:
                age = passenger['age']
                JSON2CleanTuple.check_age(age)
            except JSON2CleanTuple.InvalidAgeException as e:
                logging.warning("Invalid age '%d' in booking for passenger "\
                                  "'%s' in booking '%s': %s - %s",
                                  age, passenger['uci'], bookings_json, e, type(e))
            except KeyError as e:
                logging.warning("Missing age in passenger data: "\
                                    "%s - %s", e, bookings_json)

            try:
                passenger_type = passenger['passengerType']
                JSON2CleanTuple.check_passenger_type(passenger_type.lower())
            except JSON2CleanTuple.InvalidPassengerTypeException as e:
                logging.warning("Invalid passengerType '%s' in booking for "\
                                  "passenger '%s' in booking '%s': %s - %s",
                                  passenger_type, passenger['uci'], bookings_json, e, type(e))
            except KeyError as e:
                logging.warning("Missing passengerType in passenger data: "\
                                    "%s - %s", e, bookings_json)

            try:
                passenger_entries.append({
                    'timestamp': timestamp,
                    'uci': uci,
                    'age': age,
                    'passenger_type': passenger_type
                    })

            # pylint: disable=broad-exception-caught
            except Exception as e:
                logging.exception("Exception while creating "\
                                  "passenger_entries in booking '%s': %s - %s",
                                  bookings_json, e, type(e))
                continue

        if not passenger_entries:
            # No valid passenger list
            return JSON2CleanTuple.Output(error='Empty passenger list after processing "\
                          "non-empty json passenger list')

        flight_entries = []
        for product in products:
            flight = None
            booking_status = None
            operating_airline = None
            origin_airport = None
            destination_airport = None
            departure_date = None
            arrival_date = None
            try:
                flight = product['flight']
            except KeyError as e:
                logging.exception("Missing flight in product in booking: "\
                                    "%s - %s", e, bookings_json)
                continue

            try:
                booking_status = product['bookingStatus']
                JSON2CleanTuple.check_booking_status(booking_status.lower())
            except JSON2CleanTuple.InvalidBookingStatusException as e:
                logging.exception("Invalid bookingStatus '%s' in product in "\
                                  "booking '%s': %s - %s",
                                  booking_status, bookings_json, e, type(e))
                continue
            except KeyError as e:
                logging.exception("Missing bookingStatus in product data: "\
                                    "%s - %s", e, bookings_json)
                continue

            try:
                operating_airline = flight['operatingAirline']
                JSON2CleanTuple.check_operating_airline(operating_airline)
            except JSON2CleanTuple.InvalidOperatingAirlineException as e:
                logging.exception("Invalid operatingAirline '%s' in flight in"\
                                  "booking '%s': %s - %s",
                                  operating_airline, bookings_json, e, type(e))
                continue
            except KeyError as e:
                logging.exception("Missing operatingAirline in flight data: "\
                                    "%s - %s", e, bookings_json)
                continue

            try:
                origin_airport = flight['originAirport']
                origin_airport = JSON2CleanTuple.check_iata_code(origin_airport)
            except JSON2CleanTuple.InvalidIATACodeException as e:
                logging.warning("Invalid originAirport '%s' in flight in "\
                                  "booking '%s': %s - %s",
                                  origin_airport, bookings_json, e, type(e))
                origin_airport = None
            except KeyError as e:
                logging.warning("Missing originAirport in flight data: "\
                                    "%s - %s", e, bookings_json)

            try:
                destination_airport = flight['destinationAirport']
                destination_airport = \
                    JSON2CleanTuple.check_iata_code(destination_airport)
            except JSON2CleanTuple.InvalidIATACodeException as e:
                logging.warning("Invalid destinationAirport '%s' in flight "\
                                "in booking '%s': %s - %s",
                                destination_airport, bookings_json, e, type(e))
                destination_airport = None
            except KeyError as e:
                logging.warning("Missing destinationAirport in flight data: "\
                                    "%s - %s", e, bookings_json)

            try:
                departure_date = flight['departureDate']
                JSON2CleanTuple.check_timestamp(departure_date)
            except JSON2CleanTuple.InvalidTimestampException as e:
                # if departureDate value is not missing,
                # it is expected to be correct
                logging.exception("Invalid departureDate '%s' in flight in "\
                                  "booking '%s': %s - %s",
                                  departure_date, bookings_json, e, type(e))
                continue
            except KeyError as e:
                # if departureDate value is missing it is accepted,
                # as the flight can be cancelled
                logging.warning("Missing departureDate in flight data: "\
                                    "%s - %s", e, bookings_json)

            try:
                arrival_date = flight['arrivalDate']
                JSON2CleanTuple.check_timestamp(arrival_date)
            except JSON2CleanTuple.InvalidTimestampException as e:
                # if arrivalDate value is not missing,
                # it is expected to be correct
                logging.exception("Invalid arrivalDate '%s' in flight in "\
                                  "booking '%s': %s - %s",
                                  arrival_date, bookings_json, e, type(e))
                continue
            except KeyError as e:
                # if arrivalDate value is missing it is accepted,
                # as the flight can be cancelled
                logging.warning("Missing arrivalDate in flight data: "\
                                    "%s - %s", e, bookings_json)


            try:
                flight_entries.append({
                    'booking_status': booking_status,
                    'operating_airline': operating_airline,
                    'origin_airport': origin_airport,
                    'destination_airport': destination_airport,
                    'departure_date': departure_date,
                    'arrival_date': arrival_date,
                })
            # pylint: disable=broad-exception-caught
            except Exception as e:
                logging.exception("Exception while processing flights in "\
                                "booking '%s': %s - %s",
                                bookings_json, e, type(e))
                continue

        if not flight_entries:
            # No valid flights
            return JSON2CleanTuple.Output(error='Empty flight list "\
                                     "after processing non-empty json "\
                                     "product list')

        logging.debug("passengers: %s", passenger_entries)
        logging.debug("flights: %s", flight_entries)
        # Build a cross product of passengers and flights to get all combinations
        entries = list(itertools.product(passenger_entries, flight_entries))

        return JSON2CleanTuple.Output(
            result = (i[0] | i[1] for i in entries) # python 3.9+
#            output = ({**i[0], **i[1]} for i in entries) # python 3+
        )

def create_beam_pipeline(
        bookings:Path,
        table_id:str,
        big_query:bool,
        pipeline_args:list[str]):
    """Create an Apache Beam pipeline to process the bookings file

    Args:
        bookings (Path): flight bookings json file
        table_id (str): destination BigQuery Table ID
        big_query (bool): if True, output shall be a BigQuery table,
                        otherwise it shall be a local file 
        pipeline_args (list[str]): Pipeline specific arguments
    """

    beam_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = beam_options) as p:

        bookings = (
            p \
            | 'Read Flight Bookings' >> ReadFromText(str(bookings.resolve())) \
            | 'Clean Bookings' >> beam.ParDo(JSON2CleanTuple()).with_outputs()
        )

        if big_query:
            (# pylint: disable=expression-not-assigned
                bookings[JSON2CleanTuple.PROPER_BOOKING] \
                | 'list to new lines' >> beam.FlatMap(lambda e: e)
                | 'Write Proper Bookings to BigQuery' >> \
                    bq.WriteToBigQuery(
                        table=table_id,
                        write_disposition =\
                            bq.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition =\
                            bq.BigQueryDisposition.CREATE_NEVER))
            (# pylint: disable=expression-not-assigned
                bookings[JSON2CleanTuple.INCORRECT_BOOKING] \
                | 'Write Incorrect Bookings to BigQuery' >>\
                    bq.WriteToBigQuery(
                        table=table_id + '_error',
                        schema={
                            "fields": [
                                {
                                    "name": "json",
                                    "type": "string",
                                    "mode": "required",
                                },
                                {
                                    "name": "error",
                                    "type": "string",
                                    "mode": "required",
                                }
                            ]
                        },
                        write_disposition =\
                            bq.BigQueryDisposition.WRITE_TRUNCATE,\
                        create_disposition =\
                            bq.BigQueryDisposition.CREATE_IF_NEEDED))
        else:
            (# pylint: disable=expression-not-assigned
                bookings[JSON2CleanTuple.PROPER_BOOKING] \
                | 'list to new lines' >> beam.FlatMap(lambda e: e)
                | 'Write Proper Bookings to File' >> \
                    WriteToText(
                        file_path_prefix='output_proper',\
                        file_name_suffix=".txt"))
            (# pylint: disable=expression-not-assigned
                bookings[JSON2CleanTuple.INCORRECT_BOOKING] \
                | 'Write Incorrect Bookings to  File' >> \
                    WriteToText(
                        file_path_prefix='output_incorrect',
                        file_name_suffix=".txt"))

def parse_command_line() -> tuple[argparse.Namespace, list[str]]:
    """Parse command line arguments

    Returns:
        tuple[argparse.Namespace, list[str]]: tuple of runner independent
        command line arguments and runner dependent variables. Independent
        arguments are parsed by argparse, whereas dependent arguments are
        parsed by Apache Beam.
    """

    parser = argparse.ArgumentParser(
        prog="bookings_pipeline",
        description="Run Apache Beam pipeline to load bookings data "\
            "into BigQuery",
        epilog="Thanks for using %(prog)s! :)",
    )
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true")
    parser.add_argument("--big_query",
                        help="If this switch is on, then output "\
                            "shall be uploaded to BigQuery, otherwise "\
                            "output shall be a local file",
                        action="store_true")
    parser.add_argument("-b", "--bookings",
                        help="absolute path to the bookings json file",
                        type=lambda p:Path(p).absolute(),
                        required=True)
    parser.add_argument("-t", "--table_id",
                        help="Target BigQuery table id, format: "\
                            "<project_id>:<dataset_id>.<table_name>, "\
                            "default: booking-data-analysis."\
                            "booking_data_analysis.bookings",
                        default="booking-data-analysis:"\
                            "booking_data_analysis.bookings")

    known_args, pipeline_args = parser.parse_known_args()
    bookings:Path = known_args.bookings

    if not bookings.exists:
        logging.error("Bookings path does not exist (%s)", bookings.resolve())
        sys.exit(1)

    if not bookings.is_file:
        logging.error("Bookings path is not a file (%s)", bookings.resolve())
        sys.exit(1)

    if bookings.suffix != '.json':
        logging.error("Bookings file is not json (%s)", bookings.resolve())
        sys.exit(1)


    return known_args, pipeline_args

def main():
    """Main entry point for the code."""

    logging.basicConfig(
        encoding='utf-8',
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S (%z)',
    )

    known_args, pipeline_args = parse_command_line()
    create_beam_pipeline(known_args.bookings,
                         known_args.table_id,
                         known_args.big_query,
                         pipeline_args)

if __name__ == "__main__":
    main()
