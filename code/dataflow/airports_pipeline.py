#!/usr/bin/env python3
# *_* coding: utf-8 *_*

"""This module performs ETL of airports data into BigQuery.

    Example command line execution:

    Output to BigQuery:
        python code/dataflow/airports_pipeline.py --big_query \
            -f data/airports/airports.dat --temp_location gs://bookings-temp

    Output to local file:
        python code/dataflow/airports_pipeline.py -f \
            data/airports/airports.dat
"""

__author__ = "Zoltán Katona, PhD"
__copyright__ = f"Copyright 2024, {__author__}"
__license__ = "BSD-3-Clause"
__version__ = "0.1.0"

import argparse
import csv
import logging
from pathlib import Path
import sys
from zoneinfo import available_timezones

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
import apache_beam.io.gcp.bigquery as bq

class CSV2CleanDict(beam.DoFn):
    """Convert fields of airport from csv format to dict"""

    # pylint: disable=W0223
    # pylint: disable=R0903

    PROPER_AIRPORT = "proper_airport"
    INCORRECT_AIRPORT = "incorrect_airport"

    class Output:
        """Helper class to collect data processing output"""
        def __init__(self, result=None, error=None):
            self.result:dict = result
            self.error:str = error

    def process(self, element:str):
        # pylint: disable=W0221
        """Process csv strings

        Args:
            element (str): A single csv string containing airport data

        Returns:
            TaggedOutput: value for tags
                - proper_airport     -   airport fields as dictionary
                - incorrect_airport -   input csv string and error message

        """
        output:CSV2CleanDict.Output = CSV2CleanDict.csv2dict(element)
        if output.result:
            yield beam.pvalue.TaggedOutput(
                    CSV2CleanDict.PROPER_AIRPORT,
                        output.result
                )
        else:
            yield beam.pvalue.TaggedOutput(
                    CSV2CleanDict.INCORRECT_AIRPORT,
                    {
                        "csv": str(element),
                        "error": str(output.error),
                    }
            )

    class InvalidIATACodeException(Exception):
        """Custom exception to signal invalid IATA code"""

    class InvalidICAOCodeException(Exception):
        """Custom exception to signal invalid ICAO code"""

    class InvalidLatitudeException(Exception):
        """Custom exception to signal invalid Latitude"""

    class InvalidLongitudeException(Exception):
        """Custom exception to signal invalid Longitude"""

    class InvalidAltitudeException(Exception):
        """Custom exception to signal invalid Altitude"""

    class InvalidTimezoneException(Exception):
        """Custom exception to signal invalid Timezone"""

    class InvalidDSTException(Exception):
        """Custom exception to signal invalid daylight savings time"""

    class InvalidTypeException(Exception):
        """Custom exception to signal invalid type"""

    class InvalidSourceException(Exception):
        """Custom exception to signal invalid source"""

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
            if not isinstance(iata_code, str):
                raise TypeError(f"IATA code '{iata_code}' is not string")
            if len(iata_code) != 3:
                raise ValueError(f"IATA code '{iata_code}' is not 3 "\
                                 "characters long")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidIATACodeException from e

    @staticmethod
    def check_icao_code(icao_code:str):
        """Check if IATA code string length is 4

        Args:
            icao_code (str): ICAO code

        Raises:
            JSON2CleanTuple.InvalidICAOCodeException: exception if
            ICAO code does not have the correct length or is not string
        """

        try:
            if not isinstance(icao_code, str):
                raise TypeError(f"ICAO code '{icao_code}' is not string")
            if len(icao_code) != 4:
                raise ValueError(f"ICAO code '{icao_code}' is not 3 "\
                                 "characters long")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidICAOCodeException from e

    @staticmethod
    def check_latitude(latitude:str):
        """Check if latitude is float and is between ±90°

        Args:
            latitude (str): latitude

        Raises:
            JSON2CleanTuple.InvalidLatitudeException: exception if
            latitude does not have the correct range or is not float
        """

        try:
            latitude = float(latitude)
            if latitude > 90 or latitude < -90:
                raise ValueError(f"Latitude '{latitude}' is not between ±90°")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidLatitudeException from e

    @staticmethod
    def check_longitude(longitude:float):
        """Check if longitude is float and is between ±180°

        Args:
            latitude (str): latitude

        Raises:
            JSON2CleanTuple.InvalidLatitudeException: exception if
            latitude does not have the correct range or is not float
        """

        try:
            longitude = float(longitude)
            if longitude > 180 or longitude < -180:
                raise ValueError(f"Longitude '{longitude}' is not "\
                                 "between ±180°")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidLongitudeException from e

    @staticmethod
    def check_altitude(altitude:str):
        """Check if altitude is float and is between
        -1641 feet (-500 meters) and 29528 feet (9000 meters)

        Args:
            altitude (str): altitude

        Raises:
            JSON2CleanTuple.InvalidAltitudeException: exception if
            altitude does not have the correct range or is not float
        """

        try:
            altitude = float(altitude)
            if altitude > 29528 or altitude < -1641:
                raise ValueError(f"Altitude '{altitude}' is not "\
                                 "between -1641 and 29528 feet")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidAltitudeException from e

    @staticmethod
    def check_timezone_hours(timezone_hours:str):
        """Check if timezone_hours is float and is between -26 and 26 hours

        Args:
            timezone_hours (str): timezone offset in hours from UTC

        Raises:
            JSON2CleanTuple.InvalidTimezoneException: exception if
            timezone_hours does not have the correct range or is not float
        """

        try:
            timezone_hours = float(timezone_hours)
            if timezone_hours < -26 or timezone_hours > 26:
                raise ValueError(f"timezone_hours '{timezone_hours}' is not "\
                                 "between 0 and 26 hours")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidTimezoneException from e

    @staticmethod
    def check_dst(dst:str):
        """Check if dst string is one of [E, A, S, O, Z, N, U]

        Args:
            dst (str): Daylight savings time [E, A, S, O, Z, N, U]

        Raises:
            JSON2CleanTuple.InvalidDSTException: exception if
            dst is not one of [E, A, S, O, Z, N, U] or is not string
        """

        try:
            if not isinstance(dst, str):
                raise TypeError(f"DST '{dst}' is not string")
            if dst.capitalize() not in ['E', 'A', 'S', 'O', 'Z', 'N', 'U']:
                raise ValueError(f"DST '{dst}' is not one of "\
                                 "[E, A, S, O, Z, N, U]")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidDSTException from e

    @staticmethod
    def check_timezone_string(timezone_string:str):
        """Check if timezone_string is valid

        Args:
            timezone_string (str): timezone string

        Raises:
            JSON2CleanTuple.InvalidTimezoneException: exception if
            timezone_string is not valid
        """

        try:
            if not isinstance(timezone_string, str):
                raise TypeError(f"timezone_string '{timezone_string}' is "\
                                "not string")
            if not timezone_string in available_timezones():
                raise ValueError(f"timezone_string '{timezone_string}' "\
                                 'is not valid')
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidTimezoneException from e

    @staticmethod
    def check_type(type_in:str):
        """Check if type is string and one of
        ['airport', 'station', 'port', 'unknown']

        Args:
            type_in (str): one of ['airport', 'station', 'port', 'unknown']

        Raises:
            JSON2CleanTuple.InvalidTypeException: exception if
            type is not one of ['airport', 'station', 'port', 'unknown']
            or is not string
        """

        try:
            if not isinstance(type_in, str):
                raise TypeError(f"type '{type_in}' is not string")
            if type_in.lower() not in \
                ['airport', 'station', 'port', 'unknown']:
                raise ValueError(f"type '{type_in}' is not one of "\
                                 "['airport', 'station', 'port', 'unknown']")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidTypeException from e

    @staticmethod
    def check_source(source:str):
        """Check if type is string and one of ['ourairports', 'legacy', 'user']

        Args:
            source (str): one of ['ourairports', 'legacy', 'user']

        Raises:
            JSON2CleanTuple.InvalidTypeException: exception if
            source is not one of ['ourairports', 'legacy', 'user']
            or is not string
        """

        try:
            if not isinstance(source, str):
                raise TypeError(f"source '{source}' is not string")
            if source.lower() not in ['ourairports', 'legacy', 'user']:
                raise ValueError(f"source '{source}' is not one of "\
                                 "['ourairports', 'legacy', 'user']")
        # pylint: disable=broad-exception-caught
        except Exception as e:
            raise CSV2CleanDict.InvalidSourceException from e

    @staticmethod
    def csv2dict(airport_csv:str) -> Output:
        """Convert a csv string of airport data into a cleaned airport data as dictionary.

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
            airport_csv (str): csv object representing airport data

        Returns:
            Output: class containing either the cleaned airport data or an error message
        """
        # pylint: disable=too-many-return-statements, too-many-branches,
        # pylint: disable=too-many-statements
        # pylint: disable=too-many-locals

        try:
            reader = csv.DictReader(
                [airport_csv],
                fieldnames = (
                    "airport_id",
                    "name",
                    "city",
                    "country",
                    "iata",
                    "icao",
                    "latitude",
                    "longitude",
                    "altitude",
                    "timezone_hours",
                    "dst",
                    "timezone_string",
                    "type",
                    "source"
                )
            )
            airport = next(reader)
        # pylint: disable=broad-exception-caught
        except Exception as e:
            logging.exception("Invalid csv: '%s': %s - %s",
                                airport_csv, e, type(e))
            return CSV2CleanDict.Output(error='Invalid csv')

        try:
            tmp = airport['airport_id']
        except KeyError as e:
            logging.exception("Missing airport_id in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing airport_id')

        try:
            tmp = airport['name']
            if not isinstance(tmp, str):
                raise TypeError("name is not a string")
        except TypeError as e:
            logging.exception("False type of name: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='name is not a string')
        except KeyError as e:
            logging.exception("Missing name in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing name')

        try:
            tmp = airport['city']
            if not isinstance(tmp, str):
                raise TypeError("city is not a string")
        except TypeError as e:
            logging.exception("False type of city: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='city is not a string')
        except KeyError as e:
            logging.exception("Missing city in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing city')

        try:
            tmp = airport['iata']
            CSV2CleanDict.check_iata_code(tmp)
        except CSV2CleanDict.InvalidIATACodeException as e:
            logging.warning("Invalid iata '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            airport['iata'] = None
        except KeyError as e:
            logging.warning("Missing iata in airport: "\
                                "%s - %s", e, airport_csv)
            airport['iata'] = None

        try:
            tmp = airport['icao']
            CSV2CleanDict.check_icao_code(tmp)
        except CSV2CleanDict.InvalidICAOCodeException as e:
            logging.warning("Invalid icao '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            airport['icao'] = None
        except KeyError as e:
            logging.warning("Missing iata in airport: "\
                                "%s - %s", e, airport_csv)
            airport['icao'] = None

        try:
            tmp = airport['latitude']
            CSV2CleanDict.check_latitude(tmp)
            # max scale is 9 for BigQuery numeric data type
            airport['latitude'] = round(float(tmp), 9)
        except CSV2CleanDict.InvalidLatitudeException as e:
            logging.exception("Invalid latitude '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            return CSV2CleanDict.Output(error='Invalid latitude')
        except KeyError as e:
            logging.exception("Missing latitude in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing latitude')

        try:
            tmp = airport['longitude']
            CSV2CleanDict.check_longitude(tmp)
            # max scale is 9 for BigQuery numeric data type
            airport['longitude'] = round(float(tmp), 9)
        except CSV2CleanDict.InvalidLongitudeException as e:
            logging.exception("Invalid longitude '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            return CSV2CleanDict.Output(error='Invalid longitude')
        except KeyError as e:
            logging.exception("Missing longitude in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing longitude')

        try:
            tmp = airport['altitude']
            CSV2CleanDict.check_altitude(tmp)
            # max scale is 9 for BigQuery numeric data type
            airport['altitude'] = round(float(tmp), 9)
        except CSV2CleanDict.InvalidAltitudeException as e:
            logging.exception("Invalid altitude '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            return CSV2CleanDict.Output(error='Invalid altitude')
        except KeyError as e:
            logging.exception("Missing altitude in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing altitude')

        try:
            tmp = airport['timezone_hours']
            CSV2CleanDict.check_timezone_hours(tmp)
            airport['timezone_hours'] = float(tmp)
        except CSV2CleanDict.InvalidTimezoneException as e:
            logging.warning("Invalid timezone_hours '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            airport['timezone_hours'] = None
        except KeyError as e:
            logging.warning("Missing timezone_hours in airport: "\
                                "%s - %s", e, airport_csv)
            airport['timezone_hours'] = None

        try:
            tmp = airport['dst']
            CSV2CleanDict.check_dst(tmp)
            airport['dst'] = tmp.capitalize()
        except CSV2CleanDict.InvalidDSTException as e:
            logging.warning("Invalid dst '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            airport['dst'] = None
        except KeyError as e:
            logging.warning("Missing dst in airport: "\
                                "%s - %s", e, airport_csv)
            airport['dst'] = None

        try:
            tmp = airport['timezone_string']
            CSV2CleanDict.check_timezone_string(tmp)
        except CSV2CleanDict.InvalidTimezoneException as e:
            logging.warning("Invalid timezone_string '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            airport['timezone_string'] = None
        except KeyError as e:
            logging.warning("Missing timezone_string in airport: "\
                                "%s - %s", e, airport_csv)
            airport['timezone_string'] = None

        try:
            tmp = airport['type']
            CSV2CleanDict.check_type(tmp)
            airport['type'] = tmp.lower()
        except CSV2CleanDict.InvalidTypeException as e:
            logging.exception("Invalid type '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            return CSV2CleanDict.Output(error='Invalid type')
        except KeyError as e:
            logging.exception("Missing type in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing type')

        try:
            tmp = airport['source']
            CSV2CleanDict.check_source(tmp)
            airport['source'] = tmp.lower()
        except CSV2CleanDict.InvalidSourceException as e:
            logging.exception("Invalid source '%s' in airport "\
                                "'%s': %s - %s",
                                tmp, airport_csv, e, type(e))
            return CSV2CleanDict.Output(error='Invalid type')
        except KeyError as e:
            logging.exception("Missing source in airport: "\
                                "%s - %s", e, airport_csv)
            return CSV2CleanDict.Output(error='Missing source')

        return CSV2CleanDict.Output(
            result = airport
        )

def create_beam_pipeline(
        file:Path,
        table_id:str,
        big_query:bool,
        pipeline_args:list[str]):
    """Create an Apache Beam pipeline to process the airports file

    Args:
        file (Path): airports csv file
        table_id (str): destination BigQuery Table ID
        big_query (bool): if True, output shall be a BigQuery table,
                        otherwise it shall be a local file 
        pipeline_args (list[str]): Pipeline specific arguments
    """

    beam_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options = beam_options) as p:

        file = (
            p \
            | 'Read Flight Bookings' >> ReadFromText(str(file.resolve())) \
            | 'Clean Bookings' >> beam.ParDo(CSV2CleanDict()).with_outputs()
        )

        if big_query:
            (# pylint: disable=expression-not-assigned
                file[CSV2CleanDict.PROPER_AIRPORT] \
                | 'Write Proper Bookings to BigQuery' >> \
                    bq.WriteToBigQuery(
                        table=table_id,
                        write_disposition =\
                            bq.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition =\
                            bq.BigQueryDisposition.CREATE_NEVER))
            (# pylint: disable=expression-not-assigned
                file[CSV2CleanDict.INCORRECT_AIRPORT] \
                | 'Write Incorrect Bookings to BigQuery' >>\
                    bq.WriteToBigQuery(
                        table=table_id + '_error',
                        schema={
                            "fields": [
                                {
                                    "name": "csv",
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
                file[CSV2CleanDict.PROPER_AIRPORT] \
                | 'Write Proper Bookings to File' >> \
                    WriteToText(
                        file_path_prefix='output_proper',\
                        file_name_suffix=".txt"))
            (# pylint: disable=expression-not-assigned
                file[CSV2CleanDict.INCORRECT_AIRPORT] \
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
        description="Run Apache Beam pipeline to load airport data "\
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
    parser.add_argument("-f", "--file",
                        help="absolute path to the airports csv file",
                        type=lambda p:Path(p).absolute(),
                        required=True)
    parser.add_argument("-t", "--table_id",
                        help="Target BigQuery table id, format: "\
                            "<project_id>:<dataset_id>.<table_name>, "\
                            "default: booking-data-analysis."\
                            "booking_data_analysis.airports",
                        default="booking-data-analysis:"\
                            "booking_data_analysis.airports")

    known_args, pipeline_args = parser.parse_known_args()
    file:Path = known_args.file

    if not file.exists:
        logging.error("Airports path does not exist (%s)", file.resolve())
        sys.exit(1)

    if not file.is_file:
        logging.error("Airports path is not a file (%s)", file.resolve())
        sys.exit(1)

    if file.suffix not in ['.csv', '.dat']:
        logging.error("Airports file is not json (%s)", file.resolve())
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
    create_beam_pipeline(known_args.file,
                         known_args.table_id,
                         known_args.big_query,
                         pipeline_args)

if __name__ == "__main__":
    main()
