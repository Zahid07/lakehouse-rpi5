.venv/bin/python -u -m duckstream_pipeline.ingest     --flush-seconds 5 --flush-rows 2000     2>&1 | tee -a ~/duckstream-accel/ingest.log

.venv/bin/python -u -m duckstream_pipeline.pipeline     --interval "3 seconds" --notify     2>&1 | tee -a ~/duckstream-accel/pipeline.log

.venv/bin/python -u app/server.py --port 8080



rm -rf ~/duckstream-accel



Pi — terminal 1: ingest

cd /home/zahid/python_scripts/lakehouse-rpi5
mkdir -p ~/engine-lake
./engine_pipeline/run_ingest.sh 2>&1 | tee -a ~/engine-lake/ingest.log
Pi — terminal 2: pipeline

cd /home/zahid/python_scripts/lakehouse-rpi5
./engine_pipeline/run.sh --interval "2 seconds" --notify 2>&1 | tee -a ~/engine-lake/pipeline.log
Pi — terminal 3: dashboard

cd /home/zahid/python_scripts/lakehouse-rpi5
ENG_ROOT=$HOME/engine-lake .venv/bin/python -u engine_app/server.py --port 8090
Laptop — terminal 1: first producer

python engine_producer.py --host 192.168.0.104 \
  --machine Karachi_ENG01 --seed 12345 \
  --schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"
Laptop — terminal 2: second producer

python engine_producer.py --host 192.168.0.104 \
  --machine Karachi_ENG02 --seed 777 \
  --schedule "bearing:120,healthy:90,looseness:120,misalignment:90"
The --machine values must differ. Both models key on [timestamp, machine], so two producers sharing a name would collide on that key and roughly half the readings would silently disappear into the GROUP BY.


python engine_producer.py --host 192.168.0.104 \
  --machine Karachi_ENG03 --seed 24601 \
  --schedule "misalignment:120,healthy:90,imbalance:120,bearing:90,looseness:90"