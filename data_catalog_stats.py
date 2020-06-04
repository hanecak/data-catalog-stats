#!/usr/bin/python
#
# Copyright (C) 2011, 2013 - 2015 Peter Hanecak <hanecak@opendata.sk>
#
# This file is part of data_catalog_stats.py.
#
# data_catalog_stats.py is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# data_catalog_stats.py is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with data_catalog_stats.py. If not, see <http://www.gnu.org/licenses/>.
#
#
# Simple stat gathering tool from some selected data catalogs.
#
# TODO:
# 1) CSV results pushed into GitHub: one simple CSV with "current" data (i.e. reformatted data-catalog-stats.out) plus "complete data" (i.e. reformatted data-catalog-stats.state) pushed to GitHub
#    (GitHub will allow others to pull, with revisions/history and also do some visualizations for the data)
# 2) better persistence (SQLite, PostgreSQL, ...)
# 3) web app + scheduling - so that it can be deployed and periodically launches on its own
# 4) GUI with tables and graphs
# 5) more data sources
# 6) Socrata loader for even more data sources (sites like https://opendata.go.ke/, https://data.sfgov.org/, https://data.cityofchicago.org/, etc.)

import datetime
import logging
import os.path
import pickle
import requests
import requests_cache
from urllib.parse import urljoin


DATA_CATALOGS = {
  # API no longer accessible => disabled
  #'catalogodatos.gub.uy': {
  #  'url': 'http://catalogodatos.gub.uy/api/'
  #},
  'dados.gov.br': {
    'url': 'http://dados.gov.br/api/'
  },
  'data.edincubator.eu': {
    'url': 'https://data.edincubator.eu/api/'
  },
  # TODO: taking a long time and then raising:
  #	ValueError: No JSON object could be decoded
  #'data.gov': {
  #  'url':	'http://catalog.data.gov/api/'
  #},
  'data.gv.at': {
    'url': 'http://www.data.gv.at/katalog/api/'
  },
  'data.gov.ie': {
    'url': 'http://data.gov.ie/api/'
  },
  'data.gov.ro': {
    'url': 'https://data.gov.ro/api/'
  },
  'data.gov.sk': {
    'url': 'https://data.gov.sk/sk/api/'
  },
  'data.gov.ua': {
    'url': 'https://data.gov.ua/api/'
  },
  # TODO: there is still CKAN at https://ckan.publishing.service.gov.uk/ but API is not accessible or not at "default" location
  #'data.gov.uk': {
  #  'url': 'http://data.gov.uk/api/'
  #},
  'datahub.io': {
    'url': 'http://datahub.io/api/'
  },
  # TODO: API is still there, but apart from version check nothing works
  #'energydata.info': {
  #  'url': 'https://energydata.info/api/'
  #},
  # TODO: still a CKAN, but API not reachable
  #'hubofdata.ru': {
  #  'url': 'http://hubofdata.ru/api/'
  #},
  'odn.opendata.sk': {
    'url': 'http://odn.opendata.sk/api/'
  },
  'opendata.hu': {
    'url': 'http://opendata.hu/api/'
  },
  # TODO: new URL, but API not available there
  #'opendata.government.bg': {
  #  'url': 'http://data.egov.bg/api/'
  #},
  # TODO: API is still there, but apart from version check nothing works
  #'opendata.praha.eu': {
  #  'url': 'http://opendata.praha.eu/api/'
  #},
  # TODO: API is still there, but apart from version check nothing works
  #'opendata.swiss': {
  #  'url': 'https://opendata.swiss/en/api/'
  #},
  'podatki.gov.si': {
    'url': 'https://podatki.gov.si/api/'
  },
  # FIXME: resource count call fails ...
  'www.data.gov.sg': {
    'url': 'https://data.gov.sg/api/'
  },
  # seems like migrated to Drupal, old API no longer working => disabling for now
  #'www.dati.gov.it': {
  #  'url': 'http://www.dati.gov.it/catalog/api/'
  #},
  'www.europeandataportal.eu': {
    'url': 'http://www.europeandataportal.eu/data/en/api/'
  },
  'www.govdata.de': {
    'url': 'https://www.govdata.de/ckan/api/'
  },
  'www.hri.fi': {
    'url': 'http://www.hri.fi/api/'
  },
  'www.opendataphilly.org': {
    'url': 'https://www.opendataphilly.org/api/'
  }
}

STATE_FILE = 'data-catalog-stats.state'

COLUMN_NAMES = [
    "source",
    "dataset_count",
    "resource_count",
    "license_count(total)",
    "open_license_count",
    "non_open_license_count"
    ]


class CkanApiV1Extractor:
    
    def __init__(self, requests_session):
        self.requests_session = requests_session


    def _make_request(self, base_url, resource, args=None):
        """
        wrapper for rest client and json libraries
        """
        
        url = urljoin(base_url, resource)
        r = self.requests_session.get(url)
        if r.status_code != 200:
            raise RuntimeError('error making request: non-OK response code: {0}'.format(r.status_code))

        data = r.json()
        return data
    
    
    def get_data(self):
        """
        Get the data from the source and put them into 'data' and 'license_data'.
        """
        
        dataset_data = {}
        license_data = {}
        
        col_names = ''
        for col_name in COLUMN_NAMES:
            col_names += col_name + "\t"
        # strip last "\t"
        col_name = col_name[:-1]
        logging.info("column names: %s" % col_names)
        
        for source_name in sorted(DATA_CATALOGS.keys()):
            # fill in default values (-1) in case this (ocasionaly) fails
            dataset_data[source_name] = {}
            dataset_data[source_name]['dataset_count'] = -1
            dataset_data[source_name]['resource_count'] = -1
            dataset_data[source_name]['license_count'] = -1
            license_data[source_name] = {}
            license_data[source_name]['open_count'] = -1
            license_data[source_name]['non_open_count'] = -1

            try:
                # check API version
                data = self._make_request(DATA_CATALOGS[source_name]['url'], "1")
                if data['version'] != 1:
                    raise Exception('API is not v1: %s' % data['version'])

                # get the dataset count
                data = self._make_request(DATA_CATALOGS[source_name]['url'], "rest/dataset");
                # print `data`
                dataset_data[source_name]['dataset_count'] = len(data)
                temp_dataset_count = len(data)

                # get the resource count
                data = self._make_request(DATA_CATALOGS[source_name]['url'], "search/resource");
                dataset_data[source_name]['resource_count'] = data['count']

                # get the license count
                data = self._make_request(DATA_CATALOGS[source_name]['url'], "rest/licenses");
                dataset_data[source_name]['license_count'] = len(data)

                # get the per-license dataset count
                # TODO: for now I do not know how to do that without querying all datasets and doing the filtering
                # on my own in the code so at least we count open vs. non-open ones
                data = self._make_request(DATA_CATALOGS[source_name]['url'], "search/dataset", args={'q':'isopen:true'});
                license_data[source_name]['open_count'] = data['count']
                license_data[source_name]['non_open_count'] = temp_dataset_count - data['count']
            except Exception as e:
                logging.error('error encountered while fetching data from %s:' % source_name)
                logging.exception(e)

            
            logging.info("%s\t%d\t%d\t%d\t%d\t%d" % (
                                              source_name,
                                              dataset_data[source_name]['dataset_count'],
                                              dataset_data[source_name]['resource_count'],
                                              dataset_data[source_name]['license_count'],
                                              license_data[source_name]['open_count'],
                                              license_data[source_name]['non_open_count']
                                              ))
            
            # print('dataset_data:')
            # print `dataset_data`
            # print('license_data:')
            # print `license_data`
            
        return (dataset_data, license_data)


class DataCatalogStats:
    
    CACHE_EXPIRE = 3600 # in seconds, use 0 for "no cache"


    def __init__(self):
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s',
            level=logging.DEBUG)

        self.requests_session = requests.Session()
        if self.CACHE_EXPIRE > 0:
            self.requests_session = requests_cache.CachedSession(xpire_after=self.CACHE_EXPIRE)

        self.state = {}
        self.current_data = ();
        self.ckan_api_v1_extractor = CkanApiV1Extractor(self.requests_session)
    
    
    def load_state(self):
        if not os.path.isfile(STATE_FILE):
            logging.info('no previous state found (%s)' % STATE_FILE)
            return
        
        state_file = open(STATE_FILE, "rb");
        self.state = pickle.load(state_file);
    
    
    def save_csv_current(self):
        """
        save current values into CSV
        """
        
        import csv
        
        with open('data-catalog-stats-current.csv', 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(COLUMN_NAMES)
            
            (dataset_data, license_data) = self.current_data
            
            for source_name in sorted(dataset_data.keys()):
                csvwriter.writerow([
                                    source_name,
                                    dataset_data[source_name]['dataset_count'],
                                    dataset_data[source_name]['resource_count'],
                                    dataset_data[source_name]['license_count'],
                                    license_data[source_name]['open_count'],
                                    license_data[source_name]['non_open_count']
                                    ])
    
    
    def save_csv_all(self):
        """
        save all values into series of CSVs
        """

        import csv

        # prepare column names, i.e. list of data catalogs
        # - option 1: dump all catalogs found in state, not just those in DATA_CATALOGS
        #sorted_data_catalog_keys = []
        #for sample_key in sorted(self.state.keys()):
        #    (dataset_data, license_data) = self.state[sample_key]
        #    for source_name in dataset_data.keys():
        #        if source_name not in sorted_data_catalog_keys:
        #            sorted_data_catalog_keys.append(source_name)
        #sorted_data_catalog_keys = sorted(sorted_data_catalog_keys)
        # - option 2: just currently used catalogs
        sorted_data_catalog_keys = sorted(DATA_CATALOGS.keys())
        column_names = ['date']
        column_names.extend(sorted_data_catalog_keys)

        # dump data series
        for series in ['dataset_count', 'resource_count', 'license_count', 'open_count', 'non_open_count']:
            with open('data-catalog-stats-all-%s.csv' % series, 'w') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(column_names)

                for sample_key in sorted(self.state.keys()):
                    row = []
                    row.append(sample_key)

                    (dataset_data, license_data) = self.state[sample_key]

                    for source_name in sorted_data_catalog_keys:
                        # here, a consequence of dumb premature
                        # optimization: data split in two tuples and thus
                        # need for workaround
                        value = -1
                        if series in ['open_count', 'non_open_count']:
                            if source_name in license_data:
                                value = license_data[source_name][series]
                        else:
                            if source_name in dataset_data:
                                value = dataset_data[source_name][series]

                        row.append(value)

                    csvwriter.writerow(row)


    def update_data(self):
        # get current data sample
        self.current_data = self.ckan_api_v1_extractor.get_data()
        
        # we're going to store one sample per day => construct the key
        sample_key = datetime.date.today().strftime('%Y%m%d')
        self.state[sample_key] = self.current_data
        
        # print 'self.state:'
        # print `self.state`
    
    
    def save_state(self):
        state_file = open(STATE_FILE, "wb");
        pickle.dump(self.state, state_file);


#
# main
#
data_catalog_stats = DataCatalogStats()
data_catalog_stats.load_state()
data_catalog_stats.update_data()
data_catalog_stats.save_csv_current()
data_catalog_stats.save_csv_all()
data_catalog_stats.save_state()
