import psycopg2
import ipfshttpclient
import pickle
import requests
import numpy as np
import json
import hashlib
import time
import os

ACCOUNT_ID = '0001'
CITY = 'pittsburgh'
DIFFERENCE_THRESHOLD = 10 # degrees C

db_conn = psycopg2.connect(host="192.168.1.26",database="weatherdb", user="weatherman", password="weather")
client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001/http')


def hash_block(block):
    """
    Creates a SHA-256 hash of a Block
    :param block: <dict> Block
    :return: <str>
    """
    # We must make sure that the Dictionary is Ordered, or we'll have inconsistent hashes
    block_string = json.dumps(block, sort_keys=True).encode()
    return hashlib.sha256(block_string).hexdigest()


def mine(ACCOUNT_ID, DIFFERENCE_THRESHOLD, db_conn, client):
    # get miners associated with my account_id
    cur = db_conn.cursor()
    sql = """SELECT miner_id FROM mining_queue WHERE account_id = %s;"""
    cur.execute(sql, (ACCOUNT_ID,))
    res = cur.fetchall()
    my_miner_list = []
    for i in range(len(res)):
        my_miner_list.append(res[i][0])

    # check if you are next up on the list
    cur = db_conn.cursor()
    sql = """SELECT * FROM chain order by block_height desc limit 1"""
    cur.execute(sql, (ACCOUNT_ID,))
    res = cur.fetchall()
    last_block_height = res[0][0]
    last_txn_list = res[0][1]
    last_block_hash = res[0][2]
    last_miner = res[0][3]
    last_timestamp = res[0][4]
    next_miner = res[0][5]

    last_block = {
        'block_height': last_block_height,
        'txn_list': last_txn_list,
        'block_hash': last_block_hash,
        'miner': last_miner,
        'next_miner': next_miner
    }

    # get full list of miners and determine who is after you
    cur = db_conn.cursor()
    sql = """SELECT miner_id FROM mining_queue;"""
    cur.execute(sql)
    res = cur.fetchall()
    full_miner_list = []
    for i in range(len(res)):
        full_miner_list.append(res[i][0])
    next_miner_idx = full_miner_list.index(next_miner)
    if (next_miner_idx + 1) >= len(full_miner_list):
        next_next_miner = full_miner_list[0]
    else:
        next_next_miner = full_miner_list[next_miner_idx+1]

    if next_miner in my_miner_list:
        print('My turn to mine!')
        # PROOF OF FORECAST
        # 1) Proof of retrievability: make sure that IPFS hash is available and not empty
        cur = db_conn.cursor()
        sql = """SELECT ipfs_addr, txn_hash FROM transaction_data WHERE confirmed = '0';"""
        cur.execute(sql, (ACCOUNT_ID,))
        txn_list = cur.fetchall()
        if len(txn_list) == 0:
            print('No transactions to confirm at this time.')
        else:
            approved_txns = []
            invalid_txns = []
            for i in range(len(txn_list)):
                try:
                    # retrieve from IPFS
                    client.get(txn_list[i][0])
                    os.rename(txn_list[i][0], 'file_to_mine.data')

                    # open pickle file
                    with open('file_to_mine.data', 'rb') as filehandle:
                        txn_data = pickle.load(filehandle)
                        txn_data = txn_data.reshape(-1,5)
                        latest_temp = txn_data[0,2]

                        dev_eui = txn_data[0][0]
                        cur = db_conn.cursor()
                        sql = """SELECT city FROM world_state WHERE dev_eui = %s;"""
                        cur.execute(sql, (dev_eui,))
                        res = cur.fetchall()
                        city = res[0][0]

                        url = "https://api.openweathermap.org/data/2.5/weather?q="+ city + "&APPID=6c58649491d71d2f06f15444e14c9223&units=metric"

                        payload = {}
                        headers = {}

                        response = requests.request("GET", url, headers=headers, data=payload)
                        r = response.json()
                        city_temp = r['main']['temp']

                        # 2) Proof of Integrity: is the hyperlocal temperature "close enough" to the city-wide temperature?
                        print('City-wide temperature (C):   ', city_temp)
                        print('Hyper-local temperature (C): ', latest_temp)
                        if np.abs(city_temp - np.float(latest_temp)) < DIFFERENCE_THRESHOLD:
                            # mine block and add to chain
                            approved_txns.append(txn_list[i][1])

                            # update transaction_data
                            cur = db_conn.cursor()
                            sql = """UPDATE transaction_data SET confirmed = '1' WHERE txn_hash = %s;"""
                            cur.execute(sql, (txn_list[i][1],))
                            db_conn.commit()

                            # update world state
                            sql = """UPDATE world_state SET ipfs_addr = %s WHERE dev_eui = %s;"""
                            cur.execute(sql, (txn_list[i][0], dev_eui,))
                            db_conn.commit()

                        else:
                            # invalid transaction
                            invalid_txns.append(txn_list[i][0])
                            cur = db_conn.cursor()
                            sql = """UPDATE transaction_data SET confirmed = '-1' WHERE txn_hash = %s;"""
                            cur.execute(sql, (txn_list[i][1],))
                            db_conn.commit()
                except EOFError:
                    invalid_txns.append(txn_list[i][0])
                    cur = db_conn.cursor()
                    sql = """UPDATE transaction_data SET confirmed = '-1' WHERE txn_hash = %s;"""
                    cur.execute(sql, (txn_list[i][1],))
                    db_conn.commit()

                # update rewards
                cur = db_conn.cursor()
                sql = """UPDATE mining_queue SET valid_txns = valid_txns + %s, invalid_txns = %s WHERE dev_eui = %s;"""
                cur.execute(sql, (len(approved_txns), len(invalid_txns), dev_eui,))
                db_conn.commit()
                os.remove('file_to_mine.data')



            if len(approved_txns) > 0:
                # assemble and mine next block
                new_block_hash = hash_block(last_block)
                cur = db_conn.cursor()
                sql = """INSERT INTO chain(txn_list, block_hash, miner, timestamp, next_miner) VALUES(%s, %s, %s, %s, %s);"""
                cur.execute(sql, (str(approved_txns), new_block_hash, next_miner, str(np.round(time.time(), 0)), next_next_miner,))
                db_conn.commit()
                block = {
                    'block_height': last_block_height + 1,
                    'txn_list': str(approved_txns),
                    'block_hash': new_block_hash,
                    'miner': next_miner,
                    'next_miner': next_next_miner
                }
                print('Block mined successfully!\n', block)

                # update rewards stats
                cur = db_conn.cursor()
                sql = """UPDATE mining_queue SET blocks_mined = blocks_mined + 1 WHERE miner_id = %s;"""
                cur.execute(sql, (next_miner,))
                db_conn.commit()
                print('Rewards Updated.')



    else:
        print('Try again next epoch')

