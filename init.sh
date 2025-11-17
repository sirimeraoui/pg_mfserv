#!/bin/bash
source .venv/bin/activate
sleep 1
sudo service docker start
sleep 1
docker start mobilitydb_py
python server.py
sleep 1
pytest -v -s