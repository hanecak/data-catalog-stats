#!/usr/bin/python
#
# Simple stat gathering tool from some selected data catalogs.
#
# TODO:
# 1) better persistence (SQLite, PostgreSQL, ...)
# 2) web app + scheduling - so that it can be deployed and periodically launches on its own
# 3) GUI with tables and graphs
# 4) more data sources

import datetime
import json
import os.path
import pickle
from python_rest_client.restful_lib import Connection


DATA_CATALOGS = {
  # TODO: taking a long time and then raising:
  #	ValueError: No JSON object could be decoded
  #'data.gov': {
  #  'url':	'http://catalog.data.gov/api/'
  #},
  'data.gv.at': {
    'url':	'http://www.data.gv.at/katalog/api/'
  },
  'data.gov.cz': {
    'url':	'http://cz.ckan.net/api/'
  },
  'data.gov.sk': {
    'url':	'http://data.gov.sk/sk/api/'
  },
  'data.gov.uk': {
    'url':	'http://data.gov.uk/api/'
  }
}

STATE_FILE = 'data-catalog-stats.state'


stats = [
#  DatasetsPerCatalog,
#  ResourcesPerCatalog,
#  LicensePercentagesPerCatalog
]


class CkanApiV1Extractor:

  '''
  wrapper for rest client and json libraries
  '''
  def _make_request(self, base_url, resource, args = None):
    conn = Connection(base_url)
    # TODO: is "headers" part necessary?
    response = conn.request_get(resource, args, headers={'Accept':'text/json'})
    data = json.loads(response['body'])
    return data
  

  '''
  Get the data from the source and put them into 'data' and 'license_data'.
  '''
  def get_data(self):
    dataset_data = {}
    license_data = {}
    
    print "source\tdataset_count\tresource_count\tlicense_count(total)\topen_license_count\tnon_open_license_count"
    for source_name in DATA_CATALOGS.keys():
      # check API version
      data = self._make_request(DATA_CATALOGS[source_name]['url'], "1")
      if data['version'] != 1:
        raise Exception('API is not v1: %s' % data['version'])
    
      dataset_data[source_name] = {}
      license_data[source_name] = {}

      # get the dataset count
      data = self._make_request(DATA_CATALOGS[source_name]['url'], "rest/dataset");
      #print `data`
      dataset_data[source_name]['dataset_count'] = len(data)
      temp_dataset_count = len(data)

      # get the resource count
      data = self._make_request(DATA_CATALOGS[source_name]['url'], "search/resource");
      dataset_data[source_name]['resource_count'] = data['count']
    
      # get the license count
      data = self._make_request(DATA_CATALOGS[source_name]['url'], "rest/licenses");
      dataset_data[source_name]['license_count'] = len(data)
    
      # get the per-license dataset count
      # TODO: for now I do not know how to do that without querying all datasets and doind the filtering
      # on my own in the code so at least we count open vs. non-open ones
      data = self._make_request(DATA_CATALOGS[source_name]['url'], "search/dataset", args={'q':'isopen:true'});
      license_data[source_name]['open_count'] = data['count']
      license_data[source_name]['non_open_count'] = temp_dataset_count - data['count']

      print "%s\t%d\t%d\t%d\t%d\t%d" % (
          source_name,
          dataset_data[source_name]['dataset_count'],
          dataset_data[source_name]['resource_count'],
          dataset_data[source_name]['license_count'],
          license_data[source_name]['open_count'],
          license_data[source_name]['non_open_count']
        )
    
    #print('dataset_data:')
    #print `dataset_data`
    #print('license_data:')
    #print `license_data`
    
    return (dataset_data, license_data)


class DataCatalogStats:

  def __init__(self):
    self.state = {}
    self.ckan_api_v1_extractor = CkanApiV1Extractor()
  

  def load_state(self):
    if not os.path.isfile(STATE_FILE):
      print 'XXX no previous state found (%s)' % STATE_FILE
      return
    
    state_file = open(STATE_FILE, "rb");
    self.state = pickle.load(state_file);
  
  
  def update_data(self):
    # get current data sampla
    current_data = self.ckan_api_v1_extractor.get_data()
    
    # we're going to storeone sample per day => construct the key
    sample_key = datetime.date.today().strftime('%Y%m%d')
    self.state[sample_key] = current_data
    
    #print 'self.state:'
    #print `self.state`


  def save_state(self):
    state_file = open(STATE_FILE, "wb");
    pickle.dump(self.state, state_file);


###
### main
###
data_catalog_stats = DataCatalogStats()
data_catalog_stats.load_state()
data_catalog_stats.update_data()
data_catalog_stats.save_state()
