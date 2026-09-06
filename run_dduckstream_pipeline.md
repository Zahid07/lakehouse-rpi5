.venv/bin/python -u -m duckstream_pipeline.ingest     --flush-seconds 5 --flush-rows 2000     2>&1 | tee -a ~/duckstream-accel/ingest.log

.venv/bin/python -u -m duckstream_pipeline.pipeline     --interval "3 seconds" --notify     2>&1 | tee -a ~/duckstream-accel/pipeline.log

.venv/bin/python -u app/server.py --port 8080



rm -rf ~/duckstream-accel