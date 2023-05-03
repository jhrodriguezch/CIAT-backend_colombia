import io
import requests
import pandas as pd
from tqdm import tqdm
import datetime as dt
import concurrent.futures
from sqlalchemy import create_engine

import time

from backend_auxiliar import data_request

####################################################################
#                                                                  #
#                         r_forecast_db.py                         #
#                                                                  #
####################################################################


class Update_forecast_db:
	def __init__(self):

		before = time.time()

		# Postgres secure data
		pgres_password       = 'pass'
		pgres_databasename   = 'gess_streamflow_co'
		self.pgres_tablename_func = lambda comid : 'f_{}'.format(comid)

		# Comid column name from postgres database
		station_table_name = 'drainage'
		station_comid_name = 'HydroID'

		# GEOGloWS Streamflow Servises dictionary
		url     = 'https://geoglows.ecmwf.int/api/ForecastEnsembles/'
		url_fun = lambda x : (url ,  {'reach_id'      : x,
				                      'return_format' : 'csv'}) 
		dict_gss_aux = {'Datetime column name'   : 'datetime',
						'Datetime column format' : '%Y-%m-%dT%H:%M:%SZ'}
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
		# comids = comids[]
		
		# Download data from comid
		with concurrent.futures.ThreadPoolExecutor(max_workers = 3) as executor:
			list(tqdm(executor.map(lambda c : self.__download_data__(c, url_fun, db), 
					 		  comids),
			 	     total = len(comids)))

		print('Delay : {} seg'.format(time.time() - before))


	def __download_data__(self, comid, url, db):
		"""
		Seriealized download function
		Input:
			comid      : str   -> comid to download
			url        : func  -> function to download data
			db         : pgres -> Postgres database
		Output :
			str        : str  -> String in bytes downloaded
		"""
		# Get data for download
		url_comid, params_comid = url(comid)

		# Make server request
		self.df = data_request(url=url_comid, params=params_comid)

		# Review number of data download
		if self.df.shape[1] != 52 or\
		   self.df.shape[0] <= 2:
			self.df = self.data_request(url=url_comid, params=params_comid)

		# Review of number of data
		# if self.df.shape[0] <= 2:
		# 	self.df = self.data_request(url=url_comid, params=params_comid) 

		# Build table name
		table_name = self.pgres_tablename_func(comid)
		
		# Insert to data
		conn = db.connect()
		self.df.to_sql(table_name, con=conn, if_exists='replace', index=True)

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
		if 'ERROR' == input_data:
			self.__df = pd.DataFrame(data = {self.dict_aux['Datetime column name'] : 3 * [pd.NaT],
											 'ensemble'                            : 3 * [float('nan')]})
		else:
			data = pd.read_csv(io.StringIO(input_data),
							   index_col   = [self.dict_aux['Datetime column name']],
							   parse_dates = [self.dict_aux['Datetime column name']],
							   date_parser = lambda x : dt.datetime.strptime(x,
																			 self.dict_aux['Datetime column format']))
			self.__df = data




if __name__ == "__main__":
	print(' Updating forecast - {} '.center(70, '-').format(dt.date.today()))
	Update_forecast_db()
	print(' Updated forecast '.center(70, '-'))

