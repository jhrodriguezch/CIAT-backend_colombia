import psycopg2
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

from backend_auxiliar import get_data_wfs

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

		# ---------- MAIN ---------- 
		# Establish connection
		db   = create_engine("postgresql+psycopg2://postgres:{0}@localhost:5432/{1}".format(pgres_password, pgres_databasename))
		conn = db.connect()

		# Build hidroshare link
		full_url = url + '?' + '&'.join([ f'{key}={value}' for key, value in params.items() ])

		# Download stations   list with id and comid
		rv = get_data_wfs(url=full_url,
						 id_HS = id_HS,
						 layer = layer)
		rv = pd.DataFrame(rv)
		rv.drop(['the_geom'], axis=1, inplace=True)

		rv.rename(columns={str(col) : str(col).lower() for col in rv.columns}, inplace=True)

		# Insert to database
		rv.to_sql(pgres_tablename, con=conn, if_exists='replace', index=False)

		# Close connection
		conn.close()


if __name__ == '__main__':
	
	print(' Station database updating. '.center(70, '-'))
	Update_station_db()
	print(' Station database updated. '.center(70, '-'))

