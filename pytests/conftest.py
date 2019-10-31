import json
import psycopg2
import pytest
from iotfunctions.db import Database

#Pytest Initialization Hooks
def pytest_configure(config):
    config.addinivalue_line('markers', 'incremental: Test markers')

#Pytest Test Running Hooks
def pytest_runtest_makereport(item, call):
    if 'incremental' in item.keywords:
        if call.excinfo is not None:
            parent = item.parent
            parent._previousfailed = item

def pytest_runtest_setup(item):
    if 'incremental' in item.keywords:
        previousfailed = getattr(item.parent, '_previousfailed', None)
        if previousfailed is not None:
            pytest.xfail('previous test failed ({})'.format(previousfailed.name))

@pytest.fixture(scope='session')
def credentials():
    '''Fixture containing credentials for testing
    Returns:
        json containing credentials
    '''
    with open('../scripts/credentials_as_dev.json', encoding='utf-8') as F:
        return json.loads(F.read())
    return None 


@pytest.fixture(scope='session')
def db_connection(credentials):
    '''
    :return: postgres connection
    '''
    db_conn_values = credentials['postgresql']

    conn_string = "  host='"     + db_conn_values['host'] + \
                  "' dbname='"   + db_conn_values['databaseName'] + \
                  "' user='"     + db_conn_values['username'] + \
                  "' password='" + db_conn_values['password'] + \
                  "' port='"     + str(db_conn_values['port']) + "'"

    conn = psycopg2.connect(conn_string)
    return conn

@pytest.fixture(scope='session')
def database_obj(credentials):
    '''Fixture containing database object for testing
    Returns:
        Database object
    '''
    return Database(credentials=credentials)
