#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import psycopg2
import sys


def main():
    #with open('../scripts/credentials_as_dev.json', encoding='utf-8') as F:
    #    credentials = json.loads(F.read())

    #    # Define our connection string

    #db_conn_values = credentials['postgresql']
    #conn_string = "host='" + db_conn_values['host'] + "' dbname='" \
    #    + db_conn_values['databaseName'] + "' user='" \
    #    + db_conn_values['username'] + "' password='" \
    #    + db_conn_values['password'] + "' port='" + str(db_conn_values['port']) + "'"


    # get a connection, if a connect cannot be made an exception will be raised here
    try:
        conn_string="host='0e899846-39a1-4b58-9b60-67cb5a0aada4.bkvfvtld0lmh0umkfi70.databases.appdomain.cloud' dbname='ibmclouddb' user='ibm_cloud_7d201f19_ffd0_475b_b058_26a76cec9905' password='04cdf453585baa96c19b5e7f65c7e2762288c3c2a6043ac059283fe38a3761f1' port='32699'"
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()
        conn.close()
        print('Connected!')
    except:
        print('Not Connected')

if __name__ == '__main__':
    main()


			
