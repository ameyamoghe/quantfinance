'''
Created on Jan 16, 2018

@author: amoghe

This module contains the core data container objects
which are used to create the usable objects in other
modules 

The core data containers:
    1) DatedDataFrame - namedtuple grouping a pandas DataFrame to a
            specific datetime and DATA_ID which serves as identifier
    2) DataFrameField - namedtuple representing one column in a pandas
            DataFrame including its name, unit of measurement, default
            value if any (can be extended to add other universal attributes)
    3) DataFrameSchema - namedtuple containing INDEX_LABEL which is
            index of a pandas DataFrame and FIELDS which is a
            dictionary of column names to corresponding
            DataFrameField namedtuples
    4) DataFrameCollection - Base Class for collection of pandas DataFrames
            corresponding to dates (e.g Panel Data); includes basic
            methods to retrieve latest DataFrame for given date and
            retrieve subset of DataFrame using passed field labels
    5) SecurityLevelData - Inherits from DataFrameCollection; adds
            requirement to have SEC_ID as main index; extends constructor
            to allow specifying UNIT and DEF_VAL for any field; adds method
            to retrieve value for passed field
'''

import bisect
from collections import namedtuple, OrderedDict

from quantfinance.utilities import utils

DatedDataFrame = namedtuple('DatedDataFrame', ['date', 'data_frame', 'DATA_ID'])
DataFrameSchema = namedtuple('DataFrameSchema', ['INDEX_LABEL', 'FIELDS'])
DataFrameField = namedtuple('DataFrameField', ['NAME', 'UNIT', 'DEF_VAL'])


class DataFrameCollection(object):
    '''Base Class pandas DataFrame container

        Main object is data - a dictionary mapping
        datetime keys to pandas DataFrames

        Initialized with a DatedDataFrame which contains
        the minimum (date, DataFrame, DATA_ID) tuple
        of information required to create a collection

        Provides following methods:
            add_data -- adds new DatedDataFrame to collection
            get_latest_data -- returns latest available
                    DataFrame
            get_dated_data_frame -- returns latest available
                    data as DatedDataFrame
            get_fields -- returns subset of DataFrame for
                    given date and list of fields
    '''

    def __init__(self, dated_df):
        self.DATA_ID = dated_df.DATA_ID
        self.data = {}
        self.add_data(dated_df)

    def add_data(self, dated_df):
        self.data[dated_df.date] = dated_df.data_frame
        self.data_dates = list(self.data.keys())
        self.data_dates.sort()

    def get_previous_date(self, date):
        '''Only used to move backward through collection to find valid data'''
        i = self.data_dates.index(date)
        if i == 0:
            raise ValueError('No Earlier Data Available')
        else:
            return self.data_dates[i-1]

    def get_last_data_date(self, date):
        '''Used to find latest available DataFrame'''
        date = utils.create_datetime(date)
        try:
            result = self.data[date]
        except KeyError:
            i = bisect.bisect_right(self.data_dates, date)
            if i and i > 0:
                return self.data_dates[i-1]
            raise ValueError('No Earlier Data Available!')
        else:
            return date

    def get_latest_data(self, date=None):
        '''Return DataFrame on or closest to date'''
        if date is None:
            date = utils.get_current_date(dt=True)
        date = self.get_last_data_date(date)
        return self.data[date]

    def get_dated_data_frame(self, date=None):
        '''Return DatedDataFrame on or closest to date'''
        if date is None:
            date = utils.get_current_date(dt=True)
        date = self.get_last_data_date(date)
        return DatedDataFrame(date, self.data[date], self.DATA_ID)

    def get_fields(self, labels, date=None, name=None):
        '''Return DatedDataFrame on date using labels'''
        if date is None:
            date = utils.get_current_date(dt=True)
        date = self.get_last_data_date(date)
        if name is None:
            name = labels[0]
        data_frame = self.get_latest_data(date)
        sel = data_frame.loc[:, labels]
        return DatedDataFrame(date, sel, name)

    def to_csv(self, date=None, columns=None):
        '''Basic wrapper to write to file using pandas to_csv()'''
        pass

class SecurityLevelData(DataFrameCollection):
    '''Most primitive self-validating type of DataFrameCollection
        requiring SEC_ID as Index Label in each DataFrame

        Initialized with a DatedDataFrame and optional
        UNIT and/or DEF_VAL which can be single values
        or dicts mapping FIELD name to corresponding VALUE

        If not specified, both UNIT and DEF_VAL are None
        by default for all FIELDS

        Extends DataFrameCollection with following methods:
            get_field_names -- returns names of FIELDS (column labels)
            get_value -- returns value for (SEC_ID, FIELD) if
                available in most recent DataFrame; else returns DEF_VAL
    '''

    def __init__(self, dated_df, unit=None, def_val=None, valid=None, required=None):
        fl = dated_df.data_frame.columns.tolist()
        units = {}
        def_vals = {}
        if unit is not None or def_val is not None:
            if type(unit) not in (str, dict):
                raise ValueError('Unit Type must be a single STR or dict!')
            elif type(unit) is str:
                units = {f: unit for f in fl}
            elif type(unit) is dict:
                units.update(unit)
            if type(def_val) not in (int, float, dict):
                raise ValueError('Default Value must be a single INT, FLOAT or dict!')
            elif type(def_val) in (int, float):
                def_vals = {f: def_val for f in fl}
            elif type(def_val) is dict:
                def_vals.update(def_val)
        # fields = {f:DataFrameField(f, units.get(f), def_vals.get(f)) for f in fl}
        fields = OrderedDict()
        for f in fl:
            fields[f] = DataFrameField(f, units.get(f), def_vals.get(f))
        self.schema = DataFrameSchema('SEC_ID', fields)
        # Require first index label to be 'SEC_ID'
        # first check, then attempt to set, raise ValueError if no SEC_ID column
        primary_ind_label = dated_df.data_frame.index.name
        if primary_ind_label is None:
            try:
                # check first label in MultiIndex
                primary_ind_label = dated_df.data_frame.index.names[0]
            except (AttributeError, KeyError):
                try:
                    dated_df.data_frame.set_index(self.schema.INDEX_LABEL, inplace=True)
                    primary_ind_label = 'SEC_ID'
                except KeyError:
                    raise ValueError("DataFrame must include 'SEC_ID' column")
        if primary_ind_label != self.schema.INDEX_LABEL:
            try:
                dated_df.data_frame.set_index(self.schema.INDEX_LABEL, inplace=True)
            except KeyError:
                raise ValueError("DataFrame must include 'SEC_ID' column")
        super().__init__(dated_df)

    def get_field_names(self):
        return tuple(self.schema.FIELDS.keys())

    def get_value(self, sec_id, field, date=None):
        df = self.get_latest_data(date)
        if field not in self.get_field_names():
            raise ValueError('%s Field not found in data schema!' % field)
        try:
            result = df.loc[sec_id, field]
        except KeyError:
            result = self.schema.FIELDS[field].DEF_VAL
        return result

