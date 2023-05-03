import io
import requests
import psycopg2
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


#############################################################
#                                                           #
#                   r_observed_data.py                      #
#                                                           #
#############################################################

class Update_historical_observed_data:
	def __init__(self):
		# Postgres user data pass
		pgres_password     = 'pass'
		pgres_databasename = 'gess_streamflow_co'
		pgres_table_name   = 'observed_streamflow_data'

		# Stations column name from postgres database
		station_table_name = 'stations'
		station_id_name    = 'ID'

		# URL from data in hydroshare	
		url_hs         = 'https://www.hydroshare.org/resource/'
		id_HS_stations = '1a02d68216f24a7fbde3669b7760652d'
		func_url_build = lambda station : url_hs +\
						 '{0}/data/contents/Discharge_Data/{1}.csv'.format(id_HS_stations, station)
		dict_names_hs = {'Datetime column name'    : 'Datetime',
                         'Datetime column format'  : '%Y-%m-%d',
						 'Data column name'        : 'Streamflow (m3/s)',
						 'Data column name prefix' : 's_'}

	
		# --------------- MAIN ------------------
		# Establish connection
		db   = create_engine("postgresql+psycopg2://postgres:{0}@localhost:5432/{1}".format(pgres_password, pgres_databasename))
		conn = db.connect()

		# Read station list
		stations = pd.read_sql('select {} from {}'.format(station_id_name.lower(), station_table_name), conn)\
                     .values\
					 .flatten()\
                     .tolist()
		
		# Read data of stations
		df = self.__data_from_hydroshare__(stations = stations,
										   func_url = func_url_build,
										   dict_names = dict_names_hs)

		# Insert to database
		df.to_sql(pgres_table_name, con=conn, if_exists='replace', index=True)

		# Close connection
		conn.close()


	def __data_from_hydroshare__(self, 
								 stations : list, 
								 func_url : "Function",
								 dict_names : dict) -> pd.DataFrame:
		"""
		Download and sort the data form hydroshare.
		Input : 
			stations   : list       -> List of the stations
			func_url   : "function" -> Function with url construccion from station name/code
			dict_names : dict       -> Dictionary with hydroshare table download.
		Output :
			pandas.DataFrame : Coupling data for all stations
		"""
		
		def download_from_comid(id):
			"""
			Download the data for hydrohare
			Input : 
				id : str        -> the id of the station.
				*args, **kwards -> Inputs of __data_from_hydroshare
			Output :
				pd.DataFrame    -> Data of station
			"""
			
			# Make and review requests
			status_fail = True
			cnt = 0
			while status_fail:
				s  = requests.get(func_url(id), verify=False)
				# Review status code
				if s.status_code <= 400:
					s = s.content
					status_fail = False
				# Review number of requests
				if cnt > 3:
					print('Error in download station {}'.format(id))
					return pd.DataFrame(data = {dict_names['Datetime column name']              : [pd.NaT],
												dict_names['Data column name prefix'] + str(id) : [float('nan')]},
											    index=0)
				cnt += 1
			
			df = pd.read_csv(io.StringIO(s.decode('utf-8')),
							 parse_dates = [dict_names['Datetime column name']],
							 date_parser = lambda x : dt.datetime.strptime(x, 
																		   dict_names['Datetime column format']))
			# Fix column name
			df.rename(columns = {dict_names['Data column name'] : \
							     dict_names['Data column name prefix'] + str(id)},
					  inplace = True)
			print('Download : {}'.format(id))
			return df
		

		# TODO : Paralellization is possible, but the station list need to be splited and others.
		# Time tested : 138 s
		# before = time.time()
		for station in stations:
			station_data = download_from_comid(station)
			try:
				rv = station_data.merge(rv, 
										on  = dict_names['Datetime column name'], 
										how = 'outer')
			except:
				rv = station_data.copy()
		# print('delay in sec: {:.2f}'.format(time.time() - before))

		# Fix index
		rv.set_index(dict_names['Datetime column name'], 
                     inplace = True)

		return rv
		


if __name__ == '__main__':
	print(' Data database updating. '.center(70, '-'))
	Update_historical_observed_data()
	print(' Data database updated. '.center(70, '-'))
