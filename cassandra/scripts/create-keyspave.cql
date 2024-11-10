CREATE KEYSPACE IF NOT EXISTS "weather"
  WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 3
  }
AND DURABLE_WRITES = false;

USE "weather";

CREATE TYPE IF NOT EXISTS "weather".precipitation (
  "rain" double,
  "snow" double
);

CREATE TYPE IF NOT EXISTS "weather".wind (
  "speed" double,
  "deg" double,
  "gust" double
);

CREATE TABLE IF NOT EXISTS "weather"."countries" (
  "id" int,
  "name" text,
  "subregion" text,
  "region" text,
  PRIMARY KEY ("id", "name")
)
WITH CLUSTERING ORDER BY ("name" ASC);

CREATE TABLE IF NOT EXISTS "weather"."cities" (
  "country_id" int,
  "city_id" bigint,
  "name" text,
  "lattitude" double,
  "longtitude" double,
  PRIMARY KEY ("country_id", "city_id")
)
WITH CLUSTERING ORDER BY ("city_id" ASC);

CREATE TABLE IF NOT EXISTS "weather"."realtime_weather" (
  "city_id" bigint,
  "year" int,
  "month" int,
  "day" int,
  "time" time,
  "main" text,
  "description" text,
  "temp" double,
  "feels_like" double,
  "temp_min" double,
  "temp_max" double,
  "pressure" double,
  "humidity" decimal,
  "sea_level" double,
  "grnd_level" double,
  "visibility" int,
  "wind" frozen<wind>,
  "precipitation" frozen<precipitation>,
  "clouds" int,
  "sunrise" time,
  "sunset" time,
  PRIMARY KEY ("city_id", "year", "month", "day", "time")
)
WITH CLUSTERING ORDER BY ("year" DESC, "month" DESC, "day" DESC, "time" DESC);

CREATE TABLE IF NOT EXISTS "weather"."daily_aggregate" (
  "city_id" bigint,
  "year" int,
  "month" int,
  "day" int,
  "sum_precipitation" counter,
  "avg_pressure" double,
  "avg_temp" double,
  "temp_min" double,
  "temp_max" double,
  PRIMARY KEY ("city_id", "year", "month", "day")
)
WITH CLUSTERING ORDER BY ("year" DESC, "month" DESC, "day" DESC);

CREATE TABLE IF NOT EXISTS "weather"."monthly_cumulative_aggregate" (
  "city_id" bigint,
  "year" int,
  "month" int,
  "sum_precipitation" counter,
  "avg_pressure" double,
  "avg_temp" double,
  "temp_min" double,
  "temp_max" double,
  PRIMARY KEY ("city_id", "year", "month")
)
WITH CLUSTERING ORDER BY ("year" DESC, "month" DESC);

CREATE TABLE IF NOT EXISTS "weather"."yearly_cumulative_aggregate" (
  "city_id" bigint,
  "year" int,
  "sum_precipitation" counter,
  "avg_pressure" double,
  "avg_temp" double,
  "temp_min" double,
  "temp_max" double,
  PRIMARY KEY ("city_id", "year")
)
WITH CLUSTERING ORDER BY ("year" DESC);