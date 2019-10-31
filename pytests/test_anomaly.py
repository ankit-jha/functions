#!/user/bin/env python3
import contextlib
import logging
from iotfunctions.enginelog import EngineLogging
from iotfunctions.db import Database
import pandas as pd
from iotfunctions import metadata
from iotfunctions import bif
from sqlalchemy import Column, Float
from iotfunctions import anomaly
from iotfunctions import pipeline as pp
import datetime as dt
import pytest
from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, DateTime

ANOMALY_TEST_DATA='Anomaly_Sample_data.csv'
ANOMALY_TEST_TABLE='anomaly_testdata'
DERIVED_METRICS_ANOMALY_TEST_TABLE='dm_anomaly_testdata'
DAILY_DERIVED_METRICS_ANOMALY_TEST_TABLE='dm_anomaly_testdata_daily'

@pytest.fixture(scope='module')
def setup_db_tables(database_obj):

    print('Setup Database Tables')
    engine = database_obj.metadata.bind
    meta = MetaData()
    derived_metrics_anomaly_test_table_test=Table(
            DERIVED_METRICS_ANOMALY_TEST_TABLE, meta,
            Column('entity_id',  String(length=255)),
            Column('key',        String(length=255)),
            Column('value_n',    Float),
            Column('value_b',    Boolean),
            Column('value_s',    String(length=255)),
            Column('value_t',    DateTime),
            Column('timestamp',  DateTime),
            Column('last_update',DateTime),
            )
    meta.create_all(engine)

    yield

    print('Delete Database Tables')
    meta.drop_all(engine)

@pytest.fixture(scope='module')
def setup_data(database_obj):
    '''Fixture containing setup for test data
    '''
    print('Setup Anomaly Data')
    df_input = pd.read_csv(ANOMALY_TEST_DATA,parse_dates=['EVT_TIMESTAMP','UPDATED_UTC'])
    columns={'TEMPERATURE':'Temperature','PRESSURE':'Pressure','DEVICEID':'deviceid','EVT_TIMESTAMP':'evt_timestamp'}
    df_input.rename(columns=columns, inplace = True)

    # Generate 5 mins of data (default) in table 'ANOMALY_TEST_TABLE' with a single additional column of TestData
    jobsettings = {}
    et = metadata.EntityType(ANOMALY_TEST_TABLE, database_obj, 
                             bif.EntityDataGenerator(output_item='my_test_gen'),
                             Column('TestData',Float()),
                             Column('Temperature',Float()),
                             Column('Pressure',Float()),
                             **jobsettings)
    
    
    df = et.generate_data(entities=['73000'],datasource=df_input[(df_input.deviceid == 'A101')],
                          datasourcemetrics = ['Temperature','Pressure'])

@pytest.mark.usefixtures("setup_db_tables","setup_data")
def test_anomaly_functions(database_obj):
    '''Test anomaly functions
    '''
    print('Start testing anomaly function')
    jobsettings = {}
    et2 = metadata.EntityType(ANOMALY_TEST_TABLE, database_obj, 
                              Column('TestData',Float()),
                              Column('Pressure',Float()),
                              Column('Temperature',Float()),
                              **jobsettings)

    # checking Temperature anomaly
    et2._functions = [anomaly.KMeansAnomalyScore('Temperature',4,'TestOut2'),
                      anomaly.SpectralAnomalyScore('Temperature',12, 'TestOut')]

    # make sure the results of the python expression is saved to the derived metrics table
    et2._data_items.append({'columnName': 'TestOut', 'columnType': 'NUMBER', 'kpiFunctionId': 22856, 
                             'kpiFunctionDto': {'output': {'name': 'TestOut'}},
                            'name': 'TestOut', 'parentDataItemName': None, 'sourceTableName': DERIVED_METRICS_ANOMALY_TEST_TABLE,
                            'transient': False,'type': 'DERIVED_METRIC'})
    et2._data_items.append({'columnName': 'TestOut2', 'columnType': 'NUMBER', 'kpiFunctionId': 22856, 
                             'kpiFunctionDto': {'output': {'name': 'TestOut2'}},
                            'name': 'TestOut2', 'parentDataItemName': None, 'sourceTableName': DERIVED_METRICS_ANOMALY_TEST_TABLE,
                            'transient': False,'type': 'DERIVED_METRIC'})
    
    # map device id to entity id for the derived metrics table
    et2._data_items.append({'columnName': 'deviceid', 'columnType': 'LITERAL', 'kpiFunctionId': None,
                             'kpiFunctionDto': {},
                             'name': 'ENTITY_ID', 'parentDataItemName': None,'sourceTableName': DERIVED_METRICS_ANOMALY_TEST_TABLE,
                             'transient': False,'type': 'METRIC'})
    
    # make sure the results of the python expression is saved to the derived metrics daily table
    et2._data_items.append({'columnName': 'TestData_max', 'columnType': 'NUMBER', 'kpiFunctionId': 22856, 
                             'kpiFunctionDto': {'output': {'name': 'TestData_max'}},
                            'name': 'TestData_max', 'parentDataItemName': None, 'sourceTableName': DAILY_DERIVED_METRICS_ANOMALY_TEST_TABLE,
                            'transient': False,'type': 'DERIVED_METRIC'})
    # map device id to entity id for the derived metrics daily table
    et2._data_items.append({'columnName': 'deviceid', 'columnType': 'LITERAL', 'kpiFunctionId': None,
                             'kpiFunctionDto': {},
                             'name': 'ENTITY_ID', 'parentDataItemName': None,'sourceTableName': DAILY_DERIVED_METRICS_ANOMALY_TEST_TABLE,
                             'transient': False,'type': 'METRIC'})

    jobsettings = {'writer_name' : pp.DataWriterSqlAlchemy, 'db': database_obj, 
                   '_db_schema': 'public', 'save_trace_to_file' : False}
    job = pp.JobController(et2, **jobsettings)
    job.execute()


    table = database_obj.get_table(DERIVED_METRICS_ANOMALY_TEST_TABLE)
    start_ts = dt.datetime.utcnow() - dt.timedelta(days=40)
    end_ts = dt.datetime.utcnow()
    df_out = database_obj.read_table(table, None, None, timestamp_col='timestamp',  start_ts=start_ts, end_ts=end_ts)
    
    df_out['timestamp'] = pd.to_datetime(df_out['timestamp'])
    df_out = df_out.set_index('timestamp')
    
    table = database_obj.get_table(ANOMALY_TEST_TABLE)
    start_ts = dt.datetime.utcnow() - dt.timedelta(days=60)
    end_ts = dt.datetime.utcnow()
    df_in = database_obj.read_table(table, None, None, None, "evt_timestamp", start_ts, end_ts)
    
    df_in['timestamp'] = pd.to_datetime(df_in['evt_timestamp'])
    df_in = df_in.set_index('timestamp')

    df_in['zscore'] = df_out[(df_out.key == 'TestOut')] [['value_n']]
    df_in['kscore'] = df_out[(df_out.key == 'TestOut2')] [['value_n']]

    zscoreI = df_in[['zscore']].to_numpy()
    kscoreI = df_in[['kscore']].to_numpy()
    zscoreb = (abs(zscoreI) > 3).astype(float)
    kscoreb = (abs(kscoreI) > 2000).astype(float)

    assert zscoreb.any()
    assert kscoreb.any()
    print('End testing anomaly function')
