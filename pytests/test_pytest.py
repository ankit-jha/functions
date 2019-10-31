#!/user/bin/env python3
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
import pytest

@pytest.mark.incremental
class TestDBConnection:
    def test_connection(self,db_connection):
        cursor = db_connection.cursor()
        print("Connected!\n")
        pass

    def test_modification(self,setup_db_tables):
        print('test modification')
        pass

@pytest.fixture(scope='module')
def setup_db_tables(database_obj):
    print('Inside setup db tables')
    engine = database_obj.metadata.bind
    meta = MetaData()
    students = Table(
       'students', meta, 
       Column('id', Integer, primary_key = True), 
       Column('name', String), 
       Column('lastname', String),
    )
    meta.create_all(engine)
    print('meta {}'.format(meta))
    yield
    meta.drop_all(engine)
    print('Drop tables')
