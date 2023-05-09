import psycopg2
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

from backend_auxiliar import get_data_wfs, get_station_info

########################################################################
#                                                                      #
#                           r_station_db.py                            #
#                                                                      #
########################################################################

class Update_station_db:
	def __init__(self):
		# Postgres secure data
		pgres_password     = 'pass'
		pgres_databasename = 'gess_streamflow_co'
		pgres_tablename    = 'stations'

		# Hydroshare wfs data to download
		## WFS geojson file
		id_HS  = 'HS-dd069299816c4f1b82cd1fb2d59ec0ab'
		layer  = 'IDEAM_Stations_v2'
		url    = f"https://geoserver.hydroshare.org/geoserver/{id_HS}/ows"
		params = {'service'     : 'WFS',
				  'version'     : '1.0.0',
				  'request'     : 'GetFeature',
				  'typeName'    : f'{id_HS}%3A{layer}',
				  'maxFeatures' : '9999'}

		# Dictionary for dataframe configuration
		self.dict_dataframe = {'Geom column' : 'the_geom',
							   'Column to remove' : ['FID_1', 'FID_1_1', 'DrainLnID', 'HydroID_1', 'Basin_Stat', 'Basin_Simu', 'Ratio_', 'Shape_Leng', 'watershed', 'subbasin', 'region'],
								'Column to rename' : {'COMID'      : 'comidant',
													  'new_COMID'  : 'comid',
													  'ID'         : 'codigo',
													  'Name'       : 'nombre',
													  'Stream_Nam' : 'rio'
													 }
							   }

		# ---------- MAIN ---------- 
		# Establish connection
		db   = create_engine("postgresql+psycopg2://postgres:{0}@localhost:5432/{1}".format(pgres_password, pgres_databasename))

		# Build hidroshare link
		full_url = url + '?' + '&'.join([ f'{key}={value}' for key, value in params.items() ])

		# Download stations list with id and comid
		rv = get_data_wfs(url=full_url,
						 id_HS = id_HS,
						 layer = layer)

		# Wfs to dataframe
		rv = pd.DataFrame(rv)
		rv = self.__fix_dataframe__(rv)

		# Include extra info in station
		station_info = get_station_info()
		station_info['CODIGO'] = station_info['CODIGO'].astype(str)
		rv['codigo'] = rv['codigo'].astype(str)
		rv = rv.merge(station_info, how='left', left_on='codigo', right_on='CODIGO')

		# Remove capitalize letters for fix columns names for database
		rv.rename(columns={str(col) : str(col).lower() for col in rv.columns}, inplace=True)

		conn = db.connect()
		try:
			# Insert to database
			rv.to_sql(pgres_tablename, con=conn, if_exists='replace', index=False)
		finally:
			# Close connection
			conn.close()


	def __fix_dataframe__(self, df):
		"""
		Fix the dataframe for the IDEAM_Stations_v2
		"""
		# Names to work
		geom_col = self.dict_dataframe['Geom column']
		rm_cols  = self.dict_dataframe['Column to remove']

		# Fix the_geom column
		rv = np.array([[float(row[0].split(',')[0]), float(row[0].split(',')[1])] for row in df[geom_col].values.tolist()])
		df['x'] = rv[:,0]
		df['y'] = rv[:,1]

		# Dropp column
		df.drop([geom_col], axis=1, inplace=True)
		df.drop(rm_cols, axis=1, inplace=True)

		df.rename(columns=self.dict_dataframe['Column to rename'], inplace=True)

		# Add Alarm column
		df['alerta'] = ['R0'] * len(df)

		return df


if __name__ == '__main__':
	
	print(' Station database updating. '.center(70, '-'))
	# Update station historical validation tool
	Update_station_db()
	print(' Station database updated. '.center(70, '-'))

