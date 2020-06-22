# Import necessary packages
import os
import folium
from folium import plugins
import psycopg2
import ipfshttpclient
import pickle
import time
from folium.features import DivIcon

db_conn = psycopg2.connect(host="192.168.1.26",database="weatherdb", user="weatherman", password="weather")
cur = db_conn.cursor()
sql = """SELECT latitude, longitude, ipfs_addr FROM world_state;"""
cur.execute(sql)
res = cur.fetchall()

client = ipfshttpclient.connect('/ip4/127.0.0.1/tcp/5001/http')

m = folium.Map(location=[res[0][0], res[0][1]])

for i in range(len(res)-1):
    ipfs_hash = res[i][2]
    client.get(ipfs_hash)

    # os.rename(ipfs_hash, 'data.data')

    with open(ipfs_hash, 'rb') as filehandle:
        data = pickle.load(filehandle)

    current_data = data[0,:]
    text = str(current_data[2])
    folium.CircleMarker(
        location=[float(res[i][0]), float(res[i][1])],
        radius=50,
        popup=str(current_data[2]),
        color="#3358FF",
        opacity=1,
        fill=True,
        fill_color="#3358FF"
    ).add_child(folium.Popup(str(current_data[2]))).add_to(m)

    folium.map.Marker(
        [float(res[i][0]), float(res[i][1])],
        icon=DivIcon(
            icon_size=(150, 36),
            icon_anchor=(0, 0),
            html='<div style="font-size: 24pt"><b>%s</b></div>' % text,
        )
    ).add_to(m)

    # time.sleep(5)
    # os.rmdir('data.data')

# Display the map
# m.render()
m.save('weather_map.html')
