import http.client
import json
from os import path
import pickle
import numpy as np
# import ipfshttpclient
# import psycopg2
import time
import hashlib

ACCOUNT_ID = '0001'
CITY = 'pittsburgh'


def update_transactions(ACCOUNT_ID, CITY, db_conn, client):
    # db_conn = psycopg2.connect(host="192.168.1.26",database="weatherdb", user="weatherman", password="weather")

    conn = http.client.HTTPSConnection('api.pipedream.com')
    conn.request("GET", '/v1/sources/dc_YGulKQ/events?limit=5', '', {
      'Authorization': 'Bearer c968c4dc5acdc9d9785a84222793e2d8',
    })

    res = conn.getresponse()

    data = res.read()
    data = data.decode("utf-8")

    data_json = json.loads(data)

    events = data_json['data']

    weather_data = []
    for i in range(len(events)):
        if events[i]['e']['method'] == 'GET':
            continue
        else:
            try:
                weather_data.append([events[i]['e']['body']['dev_eui'],
                                     float(events[i]['e']['body']['reported_at']),
                                     float(events[i]['e']['body']['decoded']['payload'][0]['value']),
                                     float(events[i]['e']['body']['decoded']['payload'][1]['value']),
                                     float(events[i]['e']['body']['decoded']['payload'][2]['value'])])
            except IndexError:
                continue

    weather_array = np.array(weather_data)

    # update weather data file for each device
    dev_eui_list = np.unique(weather_array[:,0])
    for id in range(len(dev_eui_list)):
        device_data = weather_array[weather_array[:,0] == dev_eui_list[id],:]
        fp = 'weather_data/' + dev_eui_list[id] + '.data'
        if path.exists(fp) is False:
            with open(fp, 'wb') as filehandle:
                pickle.dump(device_data, filehandle)
            filehandle.close()
        else:
            with open(fp, 'rb') as filehandle:
                my_data = pickle.load(filehandle)
                last_timestamp = my_data[:,1].astype(float).max()
            filehandle.close()
            appended_data = None
            for i in range(len(device_data)):
                if device_data[i, 1].astype(float) > last_timestamp:
                    appended_data = np.append(my_data, device_data[i, :].reshape(1, len(device_data[i, :])), axis=0)
            if appended_data is not None:
                with open(fp, 'wb') as filehandle:
                    pickle.dump(appended_data, filehandle)
                    filehandle.close()

        # add to ipfs
        # client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001/http')
        res = client.add(fp)
        print(res)

        # check if data changed
        try:
            cur = db_conn.cursor()
            sql = """SELECT ipfs_addr FROM world_state WHERE dev_eui = %s;"""
            cur.execute(sql, (dev_eui_list[id],))
            old_addr = cur.fetchall()[0][0]
        except IndexError:
            old_addr = []

        if len(old_addr) == 0:
            # add device to mining pool
            sql = """INSERT INTO mining_queue(dev_eui, account_id, blocks_mined, valid_txns, invalid_txns) VALUES(%s, %s, %s, %s, %s);"""
            cur.execute(sql, (dev_eui_list[id], ACCOUNT_ID, 0, 0, 0))
            db_conn.commit()

            # update world state
            sql = """INSERT INTO world_state(dev_eui, latitude, longitude, ipfs_addr, account_id, city) VALUES(%s, %s, %s, %s, %s, %s);"""
            cur.execute(sql, (dev_eui_list[id], 'lat', 'lon', res['Hash'], ACCOUNT_ID, CITY))
            db_conn.commit()

            # insert as transaction
            sql = """INSERT INTO transaction_data(account_id, timestamp, ipfs_addr, confirmed, txn_hash) VALUES(%s, %s, %s, %s, %s);"""
            ts = str(np.round(time.time()))
            txn = ACCOUNT_ID + ts + res['Hash'] + '0'
            cur.execute(sql, (ACCOUNT_ID, ts, res['Hash'], '0', hashlib.sha256(txn.encode()).hexdigest()))
            db_conn.commit()
            print('Inserted data for new device and added to mining pool.')
        elif old_addr != res['Hash']:
            # sql = """UPDATE world_state SET ipfs_addr = %s WHERE dev_eui = %s;"""
            # cur.execute(sql, (res['Hash'], dev_eui_list[id],))
            # db_conn.commit()

            # insert as transaction
            sql = """INSERT INTO transaction_data(account_id, timestamp, ipfs_addr, confirmed, txn_hash) VALUES(%s, %s, %s, %s, %s);"""
            ts = str(np.round(time.time()))
            txn = ACCOUNT_ID + ts + res['Hash'] + '0'
            cur.execute(sql, (ACCOUNT_ID, ts, res['Hash'], '0', hashlib.sha256(txn.encode()).hexdigest()))
            db_conn.commit()
            print('Updated ipfs_address.')
        else:
            print('Nothing to change.')




