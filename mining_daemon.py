from webhook_retrieval import update_transactions
from miner import mine
import psycopg2
import ipfshttpclient
import time

ACCOUNT_ID = '0001'
CITY = 'pittsburgh'
DIFFERENCE_THRESHOLD = 10 # degrees C

db_conn = psycopg2.connect(host="192.168.1.26",database="weatherdb", user="weatherman", password="weather")
ipfs_client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001/http')

while True:
    update_transactions(ACCOUNT_ID=ACCOUNT_ID, CITY=CITY, db_conn=db_conn, client=ipfs_client)
    time.sleep(10)
    mine(ACCOUNT_ID, DIFFERENCE_THRESHOLD, db_conn, ipfs_client)
    time.sleep(60*10)