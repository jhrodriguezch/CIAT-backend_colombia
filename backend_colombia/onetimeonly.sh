#! /bin/bash

# One time only file
# This routine prepare the database of postgres
# is the first routine to run after instalation of
# postgres

cd ~/backend_colombia
conda activate gess

python r_streamflow_db.py
python r_station_db.py
python r_observed_data_db.py:
