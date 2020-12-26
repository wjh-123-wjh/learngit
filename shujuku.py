#-*-coding:utf-8 -*-
from __future__ import print_function

import paho.mqtt.client as mqtt
import struct
import json
import datetime
import serial
import time
import re
import sqlite3
# CONNECT 方式：
# client_id:     DEV_ID
# username:  PRO_ID
# password:   AUTHINFO(鉴权信息)
# 可以连接上设备云，CONNECT 和 CONNACK握手成功
# temperature:已创建的一个数据流
#更多请查阅OneNet官方mqtt文档与paho-mqtt开发文档

#修改成自己的即可
DEV_ID = "635684896" #设备ID
PRO_ID = "375526" #产品ID
AUTH_INFO = "mgt7gcxM4jwFihem3bH1uq8eQE4="  #APIKEY
#AUTH_INFO = "159635"
time = datetime.datetime.now().isoformat()
#file = open("/home/pi/ZB_data/tmp_data1.txt")
"""
con = sqlite3.connect('gw.db')
c = con.cursor()
sql_str = '''CREATE TABLE DEVICES(
            ID INTGER PRIMARY KEY ,
            TIME TEXT,
            TEMP INTEGER
);'''
c.execute(sql_str) 
con.commit()
con.close()
"""
def receive():
   # global humidity
   # with open('/home/pi/Desktop/20181031216/hjj/1.txt') as file:
    #    humidity = str(file.read())
#humidity= float(10)
    ser = serial.Serial("/dev/" + "ttyUSB0", baudrate=115200, timeout=2)
    line_data = ser.readline()
    data = line_data.decode('UTF-8')
  
    #print(data)
    read=data
    
    fdata = open('/home/pi/Desktop/20181031216/hjj/1.txt', 'w')#记得改路径
    fdata.write(read)
    fdata.close()

    data_file=open('/home/pi/Desktop/20181031216/hjj/1.txt','r')#改路径
    data1=data_file.readlines()
    print(data1)   
    data2=data1[0]
    data3=re.findall(r"\d+\.?\d*",data2)    
    data4=data3[0]
    data9=str(data4)
    print(type(data9))
    
    con = sqlite3.connect('gw.db')
    c = con.cursor()
    c.execute("INSERT INTO DEVICES(TEMP) VALUES('{}')".format(data9))
    con.commit()
    con.close
    
    con=sqlite3.connect('gw.db')
    c = con.cursor()
    sql_str = "SELECT *FROM DEVICES"
    c.execute(sql_str)
    for row in c:
        data6=list(row)
    data5=data6[2]
    con.close()
    print(data5)
    return data5


#time = datetime.datetime.now().isoformat()
TYPE_JSON = 0x01
TYPE_FLOAT = 0x17

#定义上传数据的json格式  该格式是oneNET规定好的  按格式修改其中变量即可
"""
body = {
        "datastreams":[
                {
                    "id":"wendu",  #对应OneNet的数据流名称
                    "datapoints":[
                        {
                            "at":time , #数据提交时间，这里可通过函数来获取实时时间
                            "value":humidity   #数据值
                            }
                        ]
                    }
                ]
            }

"""
def build_payload(type, payload):
    datatype = type
    packet = bytearray()
    packet.extend(struct.pack("!B", datatype))
    if isinstance(payload, str):
        udata = payload.encode('utf-8')
        length = len(udata)
        packet.extend(struct.pack("!H" + str(length) + "s", length, udata))
    return packet

# 当客户端收到来自服务器的CONNACK响应时的回调。也就是申请连接，服务器返回结果是否成功等
def on_connect(client, userdata, flags, rc,):
    read = receive()
    print("连接结果:" + mqtt.connack_string(rc))
    #上传数据
    #time.sleep(3)
    body = {
        "datastreams": [
            {
                "id": "wendu",  # 对应OneNet的数据流名称
                "datapoints": [
                    {
                        "at": time,  # 数据提交时间，这里可通过函数来获取实时时间
                        "value": read # 数据值
                    }
                ]
            }
        ]
    }


    json_body = json.dumps(body)
    packet = build_payload(TYPE_JSON, json_body)
    client.publish("$dp", packet, qos=1)  #qos代表服务质量


# 从服务器接收发布消息时的回调。
# def on_message(client, userdata, msg):
   #  print("温度:"+str(msg.payload,'utf-8')+"°C")


#当消息已经被发送给中间人，on_publish()回调将会被触发
def on_publish(client, userdata, mid):
    print("mid:" + str(mid))
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    client.username_pw_set(username=PRO_ID, password=AUTH_INFO)
    client.connect('183.230.40.39', port=6002, keepalive=120)


def main():
    client = mqtt.Client(client_id=DEV_ID, protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_publish = on_publish

    client.username_pw_set(username=PRO_ID, password=AUTH_INFO)
    client.connect('183.230.40.39', port=6002, keepalive=120)

    client.loop_forever()

if __name__ == '__main__':
    main()

