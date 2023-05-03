import io
import requests
import pandas as pd
import datetime as dt
from tqdm import tqdm
import concurrent.futures
from sqlalchemy import create_engine

import time

from backend_auxiliar import data_request
import sys

####################################################################
#                                                                  #
#                     r_forecast_record_db.py                      #
#                                                                  #
####################################################################


class Update_forecast_record_db:
	def __init__(self):

		before = time.time()

		# Postgres secure data
		pgres_password       = 'pass'
		pgres_databasename   = 'gess_streamflow_co'
		self.pgres_tablename_func = lambda comid : 'fr_{}'.format(comid)

		# Comid column name from postgres database
		station_table_name = 'drainage'
		station_comid_name = 'HydroID'

		# GEOGloWS Streamflow Servises dictionary
		url     = 'https://geoglows.ecmwf.int/api/ForecastRecords/' 
		url_fun = lambda x : (url ,  {'reach_id'      : x,
				                      'return_format' : 'csv'}) 
		dict_gss_aux = {'Datetime column name'    : 'datetime',
						'Datetime column format'  : '%Y-%m-%dT%H:%M:%SZ',
						'Data column name'        : 'streamflow_m^3/s',
						'Data column name prefix' : 'c_',
						'Days to download'        : 7, # 0 or neg value -> Download all data
						}
		self.dict_aux = dict_gss_aux

		# ------------------- MAIN --------------------
		# Establish connection
		db   = create_engine("postgresql+psycopg2://postgres:{0}@localhost:5432/{1}".format(pgres_password, 
 																							pgres_databasename))
	
		# Read comid list
		conn = db.connect()
		comids = pd.read_sql('select {} from {}'.format(station_comid_name, station_table_name), conn)\
				   .values\
				   .flatten()\
				   .tolist()
		conn.close()

		# In case of one comid is requiered, only remove the comment simbol (#) and in the list add the
		# comid to call
		# comids = comids[:20]

		# Build start date
		start_date = dt.date.today() - dt.timedelta(days=dict_gss_aux['Days to download']) 
		start_date = start_date.strftime('%Y%m%d')

		# Download data
		with concurrent.futures.ThreadPoolExecutor(max_workers = 3) as executor:
			list(tqdm(executor.map(lambda c : self.__download_data__(c, url_fun, dict_gss_aux, start_date, db),
								   comids),
					  total=len(comids)))

		print('Delay : {} seg.'.format(time.time() - before))

	
	def __download_data__(self, 
						  comid : str, 
						  url : "func", 
						  dict_aux : dict, 
						  start_date: str,
						  db : "POSTGRES database"):
		"""
		Seriealized download function
		Input:
			comid      : str  -> comid to download
			url        : func -> function to download data
			dict_aux   : dict -> Dictionary auxiliar
			start_date : str  -> Date to start the data
			db         : pgdb -> Postgres database
		"""
		# print('Downloding : {}'.format(comid))
	
		# Get data for download
		url_comid, params_comid = url(comid)

		# Fix days to download
		if dict_aux['Days to download'] > 0:
			params_comid['start_date'] = start_date #YYYYMMDD
		
		# Make server request
		self.df = data_request(url=url_comid, params=params_comid)

		# Review of number of data
		if self.df.shape[0] <= 2:
			self.df = data_request(url=url_comid, params=params_comid)

		# Fix column names
		self.df.rename(columns = {dict_aux['Data column name'] : \
								  dict_aux['Data column name prefix'] + str(comid)},
					   inplace = True)

		# Insert to database
		conn = db.connect()
		self.df.to_sql(self.pgres_tablename_func(comid), con=conn, if_exists='replace', index=True)

		# Close connection
		conn.close()

		print('Download : {}'.format(comid))

		return 0


	# Other methods
	@property
	def df(self):
		return self.__df
	@df.setter
	def df(self, input_data):
		if 'ERROR' == input_data :
			self.__df = pd.DataFrame(data = {self.dict_aux['Datetime column name'] : 3 * [pd.NaT],
										     self.dict_auc['Data column name prefix'] : 3 * [float('nan')]})
		else:
			data = pd.read_csv(io.StringIO(input_data),
							   # index_col   = [self.dict_aux['Datetime column name']],
							   parse_dates = [self.dict_aux['Datetime column name']],
							   date_parser = lambda x : dt.datetime.strptime(x,
																			 self.dict_aux['Datetime column format']))
			self.__df = data 


if __name__ == "__main__":
	print('Updating forecast record - {}'.center(70, '-').format(dt.date.today()))
	Update_forecast_record_db()
	print('Updated forecast record'.center(70, '-'))

