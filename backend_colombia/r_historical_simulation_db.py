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
#                  r_historical_simulation_db.py                   #
#                                                                  #
####################################################################

class Update_historical_simulation_db:
	def __init__(self):

		before = time.time()

		# Postgres secure data
		pgres_password       = 'pass'
		pgres_databasename   = 'gess_streamflow_co'
		self.pgres_tablename_func = lambda comid : 'hs_{}'.format(comid)

		# Comid column name from postgres database
		station_table_name = 'drainage'
		station_comid_name = 'HydroID'

		# GEOGloWS Streamflow Servises dictionary
		url     = 'https://geoglows.ecmwf.int/api/HistoricSimulation/' 
		url_fun = lambda x : (url ,  {'reach_id'      : x,
									  'forcing' : 'era_5',
				                      'return_format' : 'csv'}) 
		self.dict_aux = {'Datetime column name'    : 'datetime',
						 'Datetime column format'  : '%Y-%m-%dT%H:%M:%SZ',
						 'Data column name'        : 'streamflow_m^3/s',
						 'Data column name prefix' : 'c_',
						 }

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
		# comids = comids[7000:]

		# Download data
		with concurrent.futures.ThreadPoolExecutor(max_workers = 3) as executor:
			list(tqdm(executor.map(lambda c : self.__download_data__(c, url_fun, db),
								   comids),
					  total=len(comids)))

		print('Delay : {} seg.'.format(time.time() - before))


	def __download_data__(self, 
						  comid : str, 
						  url_fun : "func",
						  db : "POSTGRES database"):
		"""
		Seriealized download function
		Input:
			comid      : str  -> comid to download
			url        : func -> function to download data
			db         : pgdb -> Postgres h
		"""
		# print('Download : {}'.format(comid))

		# Get data for download
		url_comid, params_comid = url_fun(comid)

		# Make a requests
		df = data_request(url=url_comid, params=params_comid)

		# .___.
		if "ERROR" == df:
			df = pd.DataFrame(data = {self.dict_aux['Datetime column name']    : 3 * [pd.NaT],
									  self.dict_aux['Data column name prefix'] : 3 * [float('nan')]})
		else:
			"""
			The next try-except block has the same function and the same result. In the try statement, the result 
			is fast but requires a correct date format. In the except statement, the date format is not needed, but 
			it is slower.
			"""
			'''
			try:
				tmp = pd.read_csv(io.StringIO(df),
				 				 parse_dates = [self.dict_aux['Datetime column name']],
								 date_parser = lambda x : dt.datetime.strptime(x,
								 											   self.dict_aux['Datetime column format']),
								 index_col   = [self.dict_aux['Datetime column name']],
								)
			except:
			'''
			# print('Review download')
			tmp = pd.read_csv(io.StringIO(df))
			tmp[self.dict_aux['Datetime column name']] = pd.to_datetime(tmp[self.dict_aux['Datetime column name']])
			tmp.set_index(self.dict_aux['Datetime column name'], inplace=True)
			
			df = tmp

		# Review number of data
		if df.shape[0] <= 2:
			df = data_request(url=url_comid, params=params_comid)

			if "ERROR" == df:
				df = pd.DataFrame(data = {self.dict_aux['Datetime column name']    : 3 * [pd.NaT],
				 						  self.dict_aux['Data column name prefix'] : 3 * [float('nan')]})
			else:
				"""
				The next try-except block has the same function and the same result. In the try statement, the result 
				is fast but requires a correct date format. In the except statement, the date format is not needed, but 
				it is slower.
				"""
				'''
				try:
					tmp = pd.read_csv(io.StringIO(df),
				 					  parse_dates = [self.dict_aux['Datetime column name']],
									  date_parser = lambda x : dt.datetime.strptime(x,
								 											 	    self.dict_aux['Datetime column format']),
									  index_col   = [self.dict_aux['Datetime column name']],
									 )
				except:
					print('Review download')
				'''
				tmp = pd.read_csv(io.StringIO(df))
				tmp[self.dict_aux['Datetime column name']] = pd.to_datetime(tmp[self.dict_aux['Datetime column name']])
				tmp.set_index(self.dict_aux['Datetime column name'], inplace=True)
			
			df = tmp


		# Fix column names
		df.rename(columns = {self.dict_aux['Data column name'] : \
						     self.dict_aux['Data column name prefix'] + str(comid)},
				  inplace = True)

		# Insert to database
		conn = db.connect()
		df.to_sql(self.pgres_tablename_func(comid), con=conn, if_exists='replace', index=True)

		# Close connection
		conn.close()

		# print('Download : {}'.format(comid))

		return 0


	@property
	def df(self):
		return self.__df
	@df.setter
	def df(self, input_data):
		if "ERROR" == input_data:
			self.__df = pd.DataFrame(data = {self.dict_aux['Datetime column name']    : 3 * [pd.NaT],
											 self.dict_aux['Data column name prefix'] : 3 * [float('nan')]})
		else:
			"""
			The next try-except block has the same function and the same result. In the try statement, the result 
			is fast but requires a correct date format. In the except statement, the date format is not needed, but 
			it is slower.
			"""
			try:
				rv = pd.read_csv(io.StringIO(input_data),
				 				 parse_dates = [self.dict_aux['Datetime column name']],
								 date_parser = lambda x : dt.datetime.strptime(x,
								 											   self.dict_aux['Datetime column format']),
								 index_col   = [self.dict_aux['Datetime column name']],
								)
			except:
				print('Review download')
				rv = pd.read_csv(io.StringIO(input_data))
				rv[self.dict_aux['Datetime column name']] = pd.to_datetime(rv[self.dict_aux['Datetime column name']])
				rv.set_index(self.dict_aux['Datetime column name'], inplace=True)
			
			self.__df = rv



if __name__ == "__main__":
	print(' Updating historical simulation - {} '.center(70, '-').format(dt.date.today()))
	Update_historical_simulation_db()
	print(' Updated historical simulation '.center(70, '-'))

	
