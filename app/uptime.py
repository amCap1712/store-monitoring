from datetime import datetime, timedelta
from uuid import UUID

from psycopg2.sql import SQL, Identifier, Literal, Composed
from sqlalchemy import Connection, text


def generate_query(report_id: UUID, range_: str, start: datetime, end: datetime) -> Composed:
    """
    Generate the report query to compute uptime for a given time period.

    :param report_id: the id of the report being generated
    :param range_: time range to generate the report for, can be 'hour', 'day' or 'week'
    :param start: the start of the time period to generate the report for
    :param end: the end of the time period to generate the report for
    :return: the SQL query that should be executed to generate the report
    """
    return SQL("""
        WITH localize_timestamps as (
            SELECT so.store_id
                 -- if the timezone is not specified, assume America/Chicago
                 , so.timestamp_utc AT TIME ZONE COALESCE(s.timezone_str, 'America/Chicago') AS local_timestamp
                 , so.status
              FROM store_observation so 
         LEFT JOIN store s
                ON s.id = so.store_id
             WHERE so.timestamp_utc AT TIME ZONE COALESCE(s.timezone_str, 'America/Chicago') >= {start}
               AND so.timestamp_utc AT TIME ZONE COALESCE(s.timezone_str, 'America/Chicago') < {end}
        ), compute_business_hours AS (
            SELECT st.store_id
                 , local_timestamp::date AS date
                 , local_timestamp::time as time
                 -- if store timing is not specified, assume open 24*7
                 , COALESCE(start_time_local, '00:00:00'::time) AS start_time_local
                 , COALESCE(end_time_local, '24:00:00'::time) AS end_time_local
                 , status
                 , (local_timestamp::time, local_timestamp::time) OVERLAPS (COALESCE(start_time_local, '00:00:00'::time), COALESCE(end_time_local, '24:00:00'::time)) as within_business_hours
              FROM localize_timestamps lt
         LEFT JOIN store_timing st
                ON lt.store_id = st.store_id
               AND st.day = EXTRACT(isodow FROM lt.local_timestamp) - 1
        ), observations_after_first AS (
                 -- this CTE applies for observations after the first one in a given business hour
            SELECT store_id
                 , date
                 , status
                 , start_time_local
                 , end_time_local
                 -- compute uptime between consecutive observations using Last Observation Carry Forward
                 , COALESCE(LEAD(time, 1) OVER (PARTITION BY store_id, date, start_time_local, end_time_local ORDER BY date, time), end_time_local) - time AS diff
              FROM compute_business_hours
             WHERE within_business_hours
        ), observations_before_first AS (
                 -- this CTE applies for the first observation in a business hours
            SELECT store_id
                 , date
                 , status
                 , start_time_local
                 , end_time_local
                 -- there is no observation before the first one so use Next Observation Carry Backward for this one
                 , first_value(time) OVER (PARTITION BY store_id, date, start_time_local, end_time_local ORDER BY date, time) - start_time_local AS diff
                 , row_number() OVER (PARTITION BY store_id, date, start_time_local, end_time_local ORDER BY date, time) AS rnum
              FROM compute_business_hours
             WHERE within_business_hours
        ), all_observations AS (
            SELECT store_id
                 , date
                 , status
                 , start_time_local
                 , end_time_local
                 , diff
              FROM observations_after_first
         UNION ALL
            SELECT store_id
                 , date
                 , status
                 , start_time_local
                 , end_time_local
                 , diff
            FROM observations_before_first
           WHERE rnum = 1
        ), compute_uptime AS (
                -- combine all observations for a date's business hours
            SELECT store_id
                 , date
                 , start_time_local
                 , end_time_local
                 , CASE
                   WHEN {range} = 'hour'
                   THEN least(SUM(CASE WHEN status = 'active' THEN diff ELSE INTERVAL '0' END), INTERVAL '60 minutes')
                   ELSE SUM(CASE WHEN status = 'active' THEN diff ELSE INTERVAL '0' END)
                   END AS uptime
                 , CASE
                   WHEN {range} = 'hour'
                   THEN least(end_time_local - start_time_local, INTERVAL '60 minutes')
                      - least(SUM(CASE WHEN status = 'active' THEN diff ELSE INTERVAL '0' END), INTERVAL '60 minutes')
                   ELSE end_time_local - start_time_local - SUM(CASE WHEN status = 'active' THEN diff ELSE INTERVAL '0' END)
                   END AS downtime
              FROM all_observations
          GROUP BY store_id
                 , date
                 , start_time_local
                 , end_time_local
        ), calculate_total_times AS (
            SELECT store_id
                 , CASE
                   WHEN {range} = 'hour'
                   THEN least(SUM(uptime), INTERVAL '60 minutes')
                   ELSE SUM(uptime)
                   END AS total_uptime
                 , CASE
                   WHEN {range} = 'hour'
                   THEN least(SUM(downtime), INTERVAL '60 minutes')
                   ELSE SUM(downtime)
                   END AS total_downtime
              FROM compute_uptime
             WHERE store_id IS NOT NULL
          GROUP BY store_id
        ) INSERT INTO report_item (report_id, store_id, {uptime_key}, {downtime_key})
               SELECT {report_id}
                    , store_id
                    , total_uptime
                    , total_downtime
                 FROM calculate_total_times
          ON CONFLICT (report_id, store_id)
            DO UPDATE
                  SET {uptime_key} = EXCLUDED.{uptime_key}
                    , {downtime_key} = EXCLUDED.{downtime_key}
    """).format(
        uptime_key=Identifier(f"uptime_last_{range_}"),
        downtime_key=Identifier(f"downtime_last_{range_}"),
        report_id=Literal(report_id),
        start=Literal(start),
        end=Literal(end),
        range=Literal(range_)
    )


def get_max_timestamp(conn: Connection) -> datetime:
    """ Find the maximum timestamp of all observations in the database """
    query = """
            -- localize timestamps and then find their max. if the timezone is not specified, assume America/Chicago
        SELECT max(so.timestamp_utc AT TIME ZONE COALESCE(s.timezone_str, 'America/Chicago')) AS maximum_ts
          FROM store_observation so
     LEFT JOIN store s
            ON s.id = so.store_id
    """
    result = conn.execute(text(query))
    return result.first().maximum_ts


def compute(conn: Connection, report_id: UUID, reference_ts: datetime):
    """ Compute the uptime report """
    end_week_ts = datetime(reference_ts.year, reference_ts.month, reference_ts.day)
    start_week_ts = end_week_ts - timedelta(days=7)
    week_query = generate_query(report_id, "week", start_week_ts, end_week_ts)
    conn.exec_driver_sql(week_query)

    end_day_ts = end_week_ts
    start_day_ts = end_day_ts + timedelta(days=-1)
    day_query = generate_query(report_id, "day", start_day_ts, end_day_ts)
    conn.exec_driver_sql(day_query)

    end_hour_ts = datetime(reference_ts.year, reference_ts.month, reference_ts.day, reference_ts.hour, 0, 0)
    start_hour_ts = end_day_ts + timedelta(hours=-1)
    hour_query = generate_query(report_id, "hour", start_hour_ts, end_hour_ts)
    conn.exec_driver_sql(hour_query)
