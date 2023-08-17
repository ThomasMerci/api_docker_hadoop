from hdfs import InsecureClient
import urllib.request
import json
import datetime
import xml.etree.ElementTree as ET
import os

client = InsecureClient('http://172.22.0.6:9870/', user='root')
local_path = 'file.txt'
client.makedirs('/module')

with open(local_path, 'w') as f:
    pass
hdfs_path = '/module/file.txt'
client.upload(hdfs_path, local_path)

os.remove(local_path)


#xml
def url_builder(city_id, city_name, country):
    user_api = 'key'
    unit = 'metric'
    if city_name:
        api = 'http://api.openweathermap.org/data/2.5/weather?q='
        full_api_url = api + city_name + ',' + country + '&mode=json&units=' + unit + '&APPID=' + user_api
    else:
        api = 'http://api.openweathermap.org/data/2.5/weather?id='
        full_api_url = api + city_id + '&mode=json&units=' + unit + '&APPID=' + user_api
    return full_api_url

def data_fetch(full_api_url):
    with urllib.request.urlopen(full_api_url) as url:
        data = json.loads(url.read().decode('utf-8'))
    return data

def time_converter(time):
    if time is None:
        return None
    return datetime.datetime.fromtimestamp(int(time)).strftime('%I:%M %p')

def data_organizer(raw_api_dict):
    return {
        'city': raw_api_dict.get('name'),
        'country': raw_api_dict.get('sys').get('country'),
        'temp': raw_api_dict.get('main').get('temp'),
        'temp_max': raw_api_dict.get('main').get('temp_max'),
        'temp_min': raw_api_dict.get('main').get('temp_min'),
        'humidity': raw_api_dict.get('main').get('humidity'),
        'pressure': raw_api_dict.get('main').get('pressure'),
        'sky': raw_api_dict['weather'][0]['main'],
        'sunrise': time_converter(raw_api_dict.get('sys').get('sunrise')),
        'sunset': time_converter(raw_api_dict.get('sys').get('sunset')),
        'wind': raw_api_dict.get('wind').get('speed'),
        'wind_deg': raw_api_dict.get('wind').get('deg'),
        'dt': time_converter(raw_api_dict.get('dt')),
        'cloudiness': raw_api_dict.get('clouds').get('all')
    }

#tout
ville = 'Valence'
pays = 'FR'
ville_id = '6453882'
full_api_url = url_builder(ville_id, ville, pays)
data = data_fetch(full_api_url)
organized_data = data_organizer(data)

# Convert the data to XML and save
xml_file_name = "weatherOpenMap.xml"
root = ET.Element("weatherOpenMap")
for key, value in organized_data.items():
    child = ET.SubElement(root, key)
    if value is not None:
        child.text = str(value)

with open(xml_file_name, "wb") as file:
    ET.ElementTree(root).write(file)


#hadoop
hadoop_address = 'http://172.22.0.6:9870/'
client = InsecureClient(hadoop_address, user='root')
hdfs_path = '/module/' + xml_file_name
client.upload(hdfs_path, xml_file_name)

os.remove(xml_file_name)

#docker build -t nom_app .
#docker run --network=réseau_hadoop nom_app




