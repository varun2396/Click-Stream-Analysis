CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM"
(
  session_id VARCHAR(60),
  user_id INTEGER,
  device VARCHAR(10),
  timeagg timestamp,
  events INTEGER,
  beginnavigation VARCHAR(32),
  endnavigation VARCHAR(32),
  beginsession VARCHAR(25),
  endsession VARCHAR(25),
  duration_sec INTEGER
);

CREATE OR REPLACE PUMP "WINDOW_PUMP_MIN" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT  STREAM
UPPER(cast("user_id" as VARCHAR(3))|| '_' ||SUBSTRING("device",1,3)||cast(UNIX_TIMESTAMP(FLOOR("clicktimestamp" TO MINUTE))/1000 as VARCHAR(20))) as session_id,
"user_id" , "device",
FLOOR("clicktimestamp" TO MINUTE),
COUNT("event") events,
first_value("event") as beginnavigation,
last_value("event") as endnavigation,
SUBSTRING(cast(min("clicktimestamp") AS VARCHAR(25)),15,19) as beginsession,
SUBSTRING(cast(max("clicktimestamp") AS VARCHAR(25)),15,19) as endsession,
TSDIFF(max("clicktimestamp"),min("clicktimestamp"))/1000 as duration_sec
FROM "SOURCE_SQL_STREAM_001"
WINDOWED BY STAGGER (
            PARTITION BY "user_id", "device", FLOOR("clicktimestamp" TO MINUTE) RANGE INTERVAL '1' MINUTE);
/*
