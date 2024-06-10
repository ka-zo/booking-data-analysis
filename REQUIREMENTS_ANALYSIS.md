# Requirements Analysis

The present document provides the requirements analysis.

## Functional Requirements

<table>
    <thead>
        <tr>
            <th>Functional Requirements</th>
            <th>Verification</th>
        </tr>
    </thead>
<tbody>
    <tr>
        <td>
        The solution should be able to run on a batch of data. The example
        data sets are pretty small, but the real-life data sets can have
        sizes of up to 100s of TBs.
        </td>
        <td>
        All used technologies (Apache Beam, Dataflow, BigQuery) are designed
        to be scalable, and handle large amounts of data. Google's Dataflow
        and BigQuery are on top also No-Ops, fully managed systems. The
        implemented solution, furthermore, can handle not just batch but
        also streaming data.  
        </td>
    </tr>
    <tr>
        <td>
        The user should be able to specify a local directory or a HDFS URI as
        input for the bookings (You can assume all the required configuration
        to connect to HDFS is stored in /etc/hadoop/conf/)
        </td>
        <td>
        The current implementation can take as input parameter local files.
        Apache Beam, however, also supports reading HDFS files using the python
        module apache_beam.io.hadoopfilesystem. Similar modules are available
        in other programming languages supported by Apache Beam. Furthermore,
        when running the code completely in the Google cloud, it is
        recommended to store the files in Cloud storage, as even Dataproc that
        can run Spark or Hadoop jobs, would access input files from there. 
        </td>
    </tr>
    <tr>
        <td>
        The user should be able to specify a start-date and end-date, and the
        solution should only calculate the top destination countries based on
        bookings within this range.
        </td>
        <td>
        The parameterized SQL data analysis script available in the
        code/bigquery/top_destination_countries_per_season_weekday_date_range.sql
        file can be executed from the command line using the bq command. The
        start and end dates can be provided as command line parameters.
        The exact same script can also be used in Looker Studio to feed
        data into a data analysis report, while providing the end-user the
        possibility to control the start and end dates of the data analysis.
        The script calculates the top destination countries per season per
        weekday in the date range specified by the end-user.
        </td>
    </tr>
    <tr>
        <td>
        We are currently only interested in KL flights departing from the
        Netherlands.
        </td>
        <td>
        The aforementioned SQL script filters only KL flights departing from
        the Netherlands.
        </td>
    </tr>
    <tr>
        <td>
        The day of the week should be based on the local timezone for the
        airport.
        </td>
        <td>
        The SQL script converts the UTC arrival time from the bookings table
        to the local timezone of the destination airport from the airports
        table.
        </td>
    </tr>
    <tr>
        <td>
        Each passenger should only be counted once per flight leg.
        </td>
        <td>
        The Apache Beam pipeline creates as many rows in the bookings table
        for each passenger as the number of flights that passenger takes.
        </td>
    </tr>
    <tr>
        <td>
        We should only count a passenger if their latest booking status is
        Confirmed.
        </td>
        <td>
        The SQL data analysis script applies a filter to analyze only the
        flights, where the booking status was confirmed. 
        </td>
    </tr>
    <tr>
        <td>
        The output should be a table where each row shows the number of
        passengers per country, per day of the week, per season. The table
        should be sorted in descending order by the number of bookings,
        grouped by season and day of the week.
        </td>
        <td>
        The SQL data analysis script outputs a table with 4 columns:
        season, weekday, destination country, passengers per country.
        The data is grouped per season per weekday per destination country.
        The results are sorted in descending order by the number of
        passengers per destination country per weekday per season.
        </td>
    </tr>
</tbody>
</table>

## Non-Functional Requirements

<table>
    <thead>
        <tr>
            <th>Non-Functional Requirements</th>
            <th>Verification</th>
        </tr>
    </thead>
<tbody>
    <tr>
        <td>
        The given booking dataset is just a sample to show the format of the
        input data, your solution should be able to take as input a directory
        location on HDFS, containing many files in the same format totaling
        TBs in size.
        </td>
        <td>
        The implemented Apache Beam pipeline currently can take only a single
        local file as input. However, the script can easily be modified to
        handle as many input files as the user wants both locally or from
        an HDFS location, as Apache Beam has many modules for integrating
        different kinds of sources.
        </td>
    </tr>
    <tr>
        <td>
        The most optimal solution is desired, so that the job takes the least
        amount of time as possible, while still being elegant and easily
        understood by fellow developers.
        </td>
        <td>
        The data processing architecture is split into multiple steps, each
        responsible for a single purpose, therefore it is easy to follow and
        understand. The Apache Beam ETL pipeline provides a scalable solution
        that can run on many runners, such as the local, or the Dataflow runner
        on Google cloud. Dataflow provides a horizontal autoscaling, no-ops,
        fully managed Apache Beam runner environment, that can process large
        amounts of data. Our data processing architecture feeds the output of
        Apache Beam from Dataflow to BigQuery for data analyis. BigQuery
        automatically scales resources based on the query workload, building
        on top of a no-ops, fully managed underlying architecture. The result
        of the data analysis can be either retrieved directly to your local
        computer, or you can take a look at the results through the Google
        Cloud Console, or a Looker Studio report can be generated directly
        on the results. 
        </td>
    </tr>
    <tr>
        <td>
        The solution should be runnable on a laptop for a small dataset, as
        well as have the ability to be deployed onto a Hadoop YARN cluster to
        handle larger datasets.
        </td>
        <td>
        The implemented solution can run locally on a laptop and also in the
        cloud. Apache Beam can handle both. However when it comes to cloud, it
        can utilize many execution engines, such as Google Dataflow or
        Apache Spark.
        <a href="https://spark.apache.org/docs/3.0.3/running-on-yarn.html">
        Apache Spark can however run on Hadoop YARN</a>.
        In addition Google Dataproc can run both Hadoop and Spark jobs,
        therefore, if it is absolutely required to use Hadoop, it is possible
        to utilize the current implementation, but Google recommends to migrate
        to Apache Beam and Dataflow, if resources are given.
        </td>
    </tr>
</tbody>
</table>

## Requirements on Deliverables

<table>
    <thead>
        <tr>
            <th>Non-Functional Requirements</th>
            <th>Verification</th>
        </tr>
    </thead>
<tbody>
    <tr>
        <td>
        All code is pushed to this repository.
        </td>
        <td>
        The present git repository contains all code.
        </td>
    </tr>
    <tr>
        <td>
        Documentation is provided in the README.md on how the batch job
        works, and how to run it. The instructions on how to run it should
        be very clear, and running the job on the example data set should
        require as little configuration as possible.
        </td>
        <td>
        Documentation is available in the README.md file, including detailed
        explanation on how to configure and run the implemented solution. 
        </td>
    </tr>
    <tr>
        <td>
        Any information, (dummy)-data, files, and other assets that are needed
        to run  this service, are provided in this repository. This includes a
        docker-compose.yml if required.
        </td>
        <td>
        All files that need to produce the required output are available in the
        present git repository.
        </td>
    </tr>
</tbody>
</table>
