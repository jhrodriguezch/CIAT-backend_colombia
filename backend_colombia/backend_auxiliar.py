import re
import requests
import pandas as pd

#                        backend_auxiliar.py
# This file save all routines for run the backend routines.
# Backend routines:
# 1. r_observed_data.py
# 2. r_station_db.py
#
#


def data_request(url, params, cnt_fail=0) -> pd.DataFrame:
	"""
	Make a recursive request form url for 3 times
	Input : 
		url    : str  -> url to make request
		params : dict -> Dictionary with the params of request
	Return:
		"ERROR"      -> When the requests does not exist
		content.text -> When success the request
	"""
	# Make recursive request routine
	data = requests.get(url, params)
	# status_code = data.status_code

	if data.status_code == 200:
		# Success condition
		rv = data.text
		data.close()
		return rv
	elif data.status_code != 200 and cnt_fail > 5:
		# Condition failure
		data.close()
		print('Download fail')
		return "ERROR"
	else:
		# Condition restart
		# print('Try : {}'.format(cnt_fail))
		data.close()
		cnt_fail += 1
		return data_request(url, params, cnt_fail=cnt_fail)


def get_data_wfs(url, id_HS, layer) -> list:
	"""
	Read wfs hydroshare data
	parametres:
	    url : str  = URL for download the ows file.
	Return:
	    rv  : list = list with the data of the WFS.
	"""
	# Extract hydroshare data
	status_fail = True
	cnt = 0
	while status_fail:
		cont = requests.get(url)
		if cont.status_code < 400:
			status_fail = False
			rv = cont.text
			cont.close()
			cont = rv
		if cnt > 3:
			cont.close()
			print('Error in {} download.'.format(url))
			return []
		cnt += 1
		

	# Split by feature
	patron_main = r"<{0}:{1}(.*?)</{0}:{1}>".format(id_HS, layer)
	data = re.findall(patron_main, cont)

	# Fix data
	rv = []
	for data_by_feature in data:
		patron_table = r'<{0}:(.*?)</{0}:'.format(id_HS)
        
		name_row  = lambda x : 'the_geom' if 'the_geom' in x \
                                          else x.split('>')[0]

		value_row = lambda x : [ii for ii in re.findall('>(.*?)<', x) \
                                                        if ',' in ii] \
                                if 'the_geom' in x else x.split('>')[1]

		data_by_feature_slice = {name_row(row) : value_row(row) for row\
                                 in re.findall(patron_table,
                                               data_by_feature)}
        
		rv.append(data_by_feature_slice)
        
	return rv

