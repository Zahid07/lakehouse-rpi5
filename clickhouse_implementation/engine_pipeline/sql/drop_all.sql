-- DROP EVERYTHING the engine pipeline created in ClickHouse. ALL DATA IS LOST:
-- raw readings, stored minutes, spectrograms, machine history, host samples.
--
-- Not run by setup.sh (only sql/0*.sql are). Use ./reset.sh to drop and then
-- recreate everything, or run this by hand (e.g. in DBeaver) and then
-- ./setup.sh. The stft_db UDF is not touched: it is a server function, holds
-- no data, and reinstalling it needs sudo.
--
-- Order: consumers and jobs first, so nothing is writing while tables go.

-- 1. stop ingest: the RabbitMQ consumer and the view that feeds the fact
DROP VIEW  IF EXISTS engine.vibration_mv SYNC;
DROP TABLE IF EXISTS engine.vibration_queue SYNC;

-- 2. stop the scheduled jobs and the arrival summary
DROP VIEW IF EXISTS engine.spectrogram_mv SYNC;
DROP VIEW IF EXISTS engine.minute_health_mv SYNC;
DROP VIEW IF EXISTS engine.machine_dim_mv SYNC;
DROP VIEW IF EXISTS engine.machine_hlp_mv SYNC;
DROP VIEW IF EXISTS engine.fact_minutes_mv SYNC;

-- 3. read-time views (views on views first)
DROP VIEW IF EXISTS engine.v_audit_minute SYNC;
DROP VIEW IF EXISTS engine.v_audit_reading SYNC;
DROP VIEW IF EXISTS engine.v_engine_minute_anomaly SYNC;
DROP VIEW IF EXISTS engine.v_engine_minute_spectrogram SYNC;
DROP VIEW IF EXISTS engine.v_engine_minute_health SYNC;
DROP VIEW IF EXISTS engine.v_fact_engine_vibration SYNC;
DROP VIEW IF EXISTS engine.calc_minute_spectrogram SYNC;
DROP VIEW IF EXISTS engine.calc_minute_health SYNC;
DROP VIEW IF EXISTS engine.machine_dim SYNC;

-- 4. tables (the data)
DROP TABLE IF EXISTS engine.engine_minute_spectrogram SYNC;
DROP TABLE IF EXISTS engine.engine_minute_health SYNC;
DROP TABLE IF EXISTS engine.machine_dim_history SYNC;
DROP TABLE IF EXISTS engine.machine_hlp SYNC;
DROP TABLE IF EXISTS engine.fact_minutes SYNC;
DROP TABLE IF EXISTS engine.fact_engine_vibration SYNC;
DROP TABLE IF EXISTS engine.host_samples SYNC;

-- 5. the database itself (also removes anything not listed above)
DROP DATABASE IF EXISTS engine SYNC;
