from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys
import can
from pymongo import MongoClient
import datetime
import asyncio
import websockets
import json
# env = sys.argv[1]
# print(env)
mongoClient = MongoClient('mongodb://admin:admin_password@127.0.0.1:27017/?authSource=admin')
db = mongoClient.cancar
collection = db.test

async def connectWebsocket():
    return await websockets.connect("ws://ec2-34-250-188-141.eu-west-1.compute.amazonaws.com:80")


async def main():
    websocket = await connectWebsocket()
    await websocket.send(json.dumps({ "value": "established", "msgId":"0"}))
    consumer = KafkaConsumer('cancar-events',
        bootstrap_servers=['ec2-34-250-188-141.eu-west-1.compute.amazonaws.com:9092'],
        api_version=(3,0,0))
    print(consumer.topics())
    for msg in consumer:
        print(msg)
        await websocket.send(json.dumps({ "value": msg.value, "msgId": msg.key}))
        collection.insert_one({"createdAt": datetime.datetime.utcnow(), "value": msg.value, "msgId": msg.key})

asyncio.run(main())
