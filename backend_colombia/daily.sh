#! /bin/bash

# Daily bash file

cd ~/backend_colombia
conda activate gess

python r_forecast_db.py
python r_forecast_record_db.py

