.venv/bin/python -u -m duckstream_pipeline.ingest     --flush-seconds 5 --flush-rows 2000     2>&1 | tee -a ~/duckstream-accel/ingest.log

.venv/bin/python -u -m duckstream_pipeline.pipeline     --interval "3 seconds" --notify     2>&1 | tee -a ~/duckstream-accel/pipeline.log

.venv/bin/python -u app/server.py --port 8080

questions to ask:
where am I seeing the live FFT values, and all of these are being performed in duckdb right? the sum, the avg, the stg. I know fft is a UDF, but that is being called from duck db right