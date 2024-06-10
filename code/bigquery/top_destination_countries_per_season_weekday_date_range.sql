SELECT
  season,
  weekday,
  destination_country,
  SUM(passengers) AS passengers_per_country
FROM (
  SELECT
    CASE
      WHEN FORMAT_DATE('%m%d', local_arrival_date) <= '0229' OR FORMAT_DATE('%m%d', local_arrival_date) >= '1201' THEN CAST('Winter' AS STRING)
      WHEN FORMAT_DATE('%m%d', local_arrival_date) >= '0301'
    AND FORMAT_DATE('%m%d', local_arrival_date) <= '0531' THEN CAST('Spring' AS STRING)
      WHEN FORMAT_DATE('%m%d', local_arrival_date) >= '0601' AND FORMAT_DATE('%m%d', local_arrival_date) <= '0831' THEN CAST('Summer' AS STRING)
      WHEN FORMAT_DATE('%m%d', local_arrival_date) >= '0901'
    AND FORMAT_DATE('%m%d', local_arrival_date) <= '1130' THEN CAST('Autumn' AS STRING)
  END
    AS season,
    local_arrival_date,
    FORMAT_DATE('%A', local_arrival_date) AS weekday,
    destination_country,
    passengers,
  FROM (
    SELECT
      DATE(bookings.arrival_date, destination_airport.timezone_string) AS local_arrival_date,
      destination_airport.country AS destination_country,
      COUNT(*) AS passengers
    FROM
      booking-data-analysis.booking_data_analysis.bookings AS bookings
    INNER JOIN
      booking-data-analysis.booking_data_analysis.airports AS origin_airport
    ON
      bookings.origin_airport = origin_airport.iata
    INNER JOIN
      booking-data-analysis.booking_data_analysis.airports AS destination_airport
    ON
      bookings.destination_airport = destination_airport.iata
    WHERE
      origin_airport.country = "Netherlands"
      AND bookings.operating_airline = 'KL'
      AND bookings.booking_status = 'CONFIRMED'
      AND destination_airport.timezone_string IS NOT NULL
      AND DATE(bookings.arrival_date) >= PARSE_DATE('%Y%m%d', @DS_START_DATE)
      AND DATE(bookings.arrival_date) <= PARSE_DATE('%Y%m%d', @DS_END_DATE)
    GROUP BY
      local_arrival_date,
      destination_country
    ORDER BY
      local_arrival_date DESC,
      passengers DESC))
GROUP BY
  season,
  weekday,
  destination_country
ORDER BY
  season DESC,
  weekday ASC,
  passengers_per_country DESC