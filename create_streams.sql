-- Source Stream
CREATE STREAM json_sensor1_source ("timestamp" BIGINT, "values" ARRAY<INTEGER>) WITH (KAFKA_TOPIC='json.sensor1', VALUE_FORMAT='JSON');

-- Sink Stream
CREATE STREAM 'avro_sensor1' WITH (VALUE_FORMAT='AVRO') AS SELECT * FROM json_sensor1_source;

-- Transform Test Stream 
CREATE STREAM avro_sensor1_stream_2 ("timestamp" BIGINT, "values" ARRAY<INTEGER>) WITH (KAFKA_TOPIC = 'avro.sensor1.v2', partitions = 1, value_format = 'avro');

-- View data

SELECT * FROM avro_sensor1_stream_2;

-- Test UDF
SELECT "values", extractMinuteIndex("timestamp", "values") AS current_value FROM avro_sensor1_stream_2 EMIT CHANGES;

-- Final Stream
CREATE STREAM avro_sensor1_stream_2 WITH (VALUE_FORMAT='AVRO') AS SELECT "timestamp", "values", extractMinuteIndex("timestamp", "values") AS current_value FROM json_sensor1_stream_source EMIT CHANGES;