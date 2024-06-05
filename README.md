# Commercial Booking Analysis

## Preparing local environment

Open your shell, and make sure you follow the present guide properly.

Make sure you clone the present git repository to your system and then
you need to enter the directory of the cloned repository in your shell.

You will also need to install the `gcloud` CLI, you need to perform
authentication and you need to set a specific project, you created before,
in order to allow running the python scripts below. All of these steps are
not explained in this document, but you can refer to the
[gcloud CLI How-To](https://cloud.google.com/sdk/docs/how-to) pages.

After this, make sure you have at least python 3.9. If you don't have python,
please install it on your system.
```bash
$ python --version
Python 3.10.11
```

Upgrade your pip version, if necessary.
``` bash
$ python -m pip install --upgrade pip
```

Create a python virtual environment and then activate it.
Make sure you use the right command for activating the virtual
environment, as it depends on your underlying operating system.
The code below shows, how to do it on Windows. Follow this link to
['venv — Creation of virtual environments'](https://docs.python.org/3/library/venv.html)
if you want to learn more about python virtual environments.

```bash
$ python -m venv venv
$ source venv/Scripts/activate
$ pip install -r requirements.txt
```

## General remarks regarding implementation

The implemented data processing system has the following architecture:

![Source -> Apache Beam (Google Dataflow) -> Data Warehouse (BigQuery)
-> Reporting (Looker Studio).](./assets/data_process_architecture.svg)

- **Source**: The source in this specific case are `data/bookings/booking.json`
and `data/airports/airports.dat` input files, that are batched processed by
Apache Beam. The location of these files in the architecture can be
anything that Apache Beam can handle. Currently they are expected to be
available locally on the computer, where Apache Beam shall run.
- **Apache Beam**: Apache Beam can perform both batch and stream
data processing, can run on many different platforms, such as Google Dataflow,
is scalable, and can integrate with many different sources and sinks. It can also
perform many different data processing functions. Please note, that as there is
no free tier of Dataflow to the best of my knowledge, therefore I decided to not
use it, however the pipeline in the script can directly or with minor
modifications (e.g. source) run on Google Dataflow.
- **Data Warehouse**: Google BigQuery is a fast Data Warehouse solution. The
Apache Beam pipeline feeds data into BigQuery tables. Apache Beam shall feed
bookings and airports data into BigQuery. For each of them, Apache Beam shall
dynamically create a table for incorrect data, that did not pass the data
cleansing process. All other, proper, clean data shall be fed into already
existing tables.
- **Reporting**: Reporting is done using Looker Studio. The report shows the most
visited countries in the user specified data range, which can also be a single
day. Please note, that Apache Beam can handle not just batch, but also stream
processing, therefore it is possible to create dynamically updating reports.

There are 3 python scripts implemented:

- `code/create_empty_tables.py`: Creates empty tables for airports and bookings
in BigQuery in your Google Cloud project. Table schemas and clustering
information are of course part of the script. Make sure you have already
created your Google Cloud project, and you have already created a dataset in
BigQuery. The reason for creating empty tables before running the pipeline, is that
it is not possible to provide clustering information in the table schema when
writing to BigQuery.
- `code/dataflow/bookings_pipeline.py`: Creates an Apache Beam pipeline and
performs ETL batch processing on the bookings input file provided as command
line parameter. The script can perform the batch processing either locally with
output to local text files, or locally with output to BigQuery tables. The
pipeline in the script can however be run on Google Dataflow and the source can
be changed. The pipeline can also perform stream processing of bookings, making
it possible to create dynamically updating reports.
- `code/dataflow/airports_pipeline.py`: Creates an Apache Beam pipeline and
performs ETL batch processing on the airports input file provided as command
line parameter. The script can perform the batch processing either locally with
output to local text files, or locally with output to BigQuery tables. The
pipeline in the script can however be run on Google Dataflow and the source can
be changed.


## Run Apache Beam pipeline locally with local output

The following command shall create two output files, one for the proper,
cleaned flight bookings, and one for the incorrect bookings. The name of
both files shall start with the word 'output'.

```bash
$ python code/dataflow/bookings_pipeline.py -f data/bookings/booking.json
```

The following code creates similar output files as before, but this time for
the airports file. Make sure those output files are either deleted, renamed
or moved to another directory, otherwise the following code shall throw an
exception, as the script would like to create output files with the same
name.

```bash
$ python code/dataflow/airports_pipeline.py -f data/airports/airports.dat
```

Exceptions might occur for the following reasons:

- Logging the reason for an incorrect booking.
- Other system or code related exceptions, such as input file not found,
output file already exists, etc.

The script might also log warnings, to report recoverable issues with data,
such as incorrect age information, which in this case shall be nullified, as
the field in the corresponding table is NULLABLE. The logging level of the
script is set to ERROR, which allows the logging interface to log only
exceptions but not warnings.

## Run Apache Beam pipeline locally with output to BigQuery

Before running the scripts, you need to create a temporary Google cloud
storage bucket to upload the data to a BigQuery table. This storage shall
be used by Dataflow and BigQuery automatically.

An example for creating such a storage is shown below:
```bash
$ gcloud storage buckets create --location europe-west3 gs://bookings-temp
```

After this, you can run the scripts below, but make sure, you replace the
BigQuery table ID provided below with your own table ID.

This command uploads the bookings:
```bash
$ python code/dataflow/bookings_pipeline.py --big_query \
-t <project_id>:<dataset_id>.<bookings_table_name> \
-f data/bookings/booking.json --temp_location gs://bookings-temp
```

This command uploads the airports:
```bash
$ python code/dataflow/airports_pipeline.py --big_query \
-t <project_id>:<dataset_id>.<airports_table_name> \
-f data/airports/airports.dat --temp_location gs://bookings-temp
```
