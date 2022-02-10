from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys
import can
from pymongo import MongoClient
import datetime
import asyncio
import websockets
import json
import base64
import logging

logging.basicConfig(filename='canconsumer.log', encoding='utf-8', level=logging.DEBUG)

# env = sys.argv[1]
# print(env)
mongoClient = MongoClient('mongodb://admin:admin_password@127.0.0.1:27017/?authSource=admin')
db = mongoClient.cancar
collection = db.test

async def connectWebsocket():
    return await websockets.connect("ws://ec2-52-19-41-28.eu-west-1.compute.amazonaws.com:80",ping_interval=None)

async def main():
    websocket = await connectWebsocket()
    print(websocket)
    consumer = KafkaConsumer('cancar-events',
        bootstrap_servers=['ec2-52-19-41-28.eu-west-1.compute.amazonaws.com:9092'],
        api_version=(3,0,0))
    print(consumer.topics())
    for msg in consumer:
        print(msg.value.decode("utf-8"))
        d = msg.value.decode("utf-8")
        obj = json.loads(d)
        print(obj)
        logging.debug(msg)
        await websocket.send(d)
        collection.insert_one({"createdAt": datetime.datetime.utcnow(), "data": obj['data'], "msgId": obj['id']})

asyncio.run(main())
