CREATE KEYSPACE IF NOT EXISTS "weather"
  WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
  }
AND DURABLE_WRITES = true;

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

CREATE TABLE IF NOT EXISTS "weather"."cities" (
  "city" text,
  "latitude" double,
  "longitude" double,
  PRIMARY KEY ("city")
);

CREATE TABLE IF NOT EXISTS "weather"."realtime_weather" (
  "city" text,
  "datetime" timestamp,
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
  "sunrise" timestamp,
  "sunset" timestamp,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);

CREATE TABLE IF NOT EXISTS "weather"."daily_precipitation_aggregate" (
  "city" text,
  "datetime" timestamp,
  "sum_precipitation" counter,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);

CREATE TABLE IF NOT EXISTS "weather"."daily_aggregate" (
  "city" text,
  "datetime" timestamp,
  "avg_pressure" double,
  "avg_temp" double,
  "temp_min" double,
  "temp_max" double,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);

CREATE TABLE IF NOT EXISTS "weather"."monthly_precipitation_aggregate" (
  "city" text,
  "datetime" timestamp,
  "sum_precipitation" counter,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);

CREATE TABLE IF NOT EXISTS "weather"."monthly_aggregate" (
  "city" text,
  "datetime" timestamp,
  "avg_pressure" double,
  "avg_temp" double,
  "temp_min" double,
  "temp_max" double,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);

CREATE TABLE IF NOT EXISTS "weather"."yearly_precipitation_aggregate" (
  "city" text,
  "datetime" timestamp,
  "sum_precipitation" counter,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);

CREATE TABLE IF NOT EXISTS "weather"."yearly_aggregate" (
  "city" text,
  "datetime" timestamp,
  "avg_pressure" double,
  "avg_temp" double,
  "temp_min" double,
  "temp_max" double,
  PRIMARY KEY ("city", "datetime")
)
WITH CLUSTERING ORDER BY ("datetime" DESC);