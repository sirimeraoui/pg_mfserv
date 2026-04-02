import asyncio
import websockets
import json
from datetime import datetime, timezone

async def connect_ais_stream():

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
             "APIKey": "a88486537fc21b50371675ac035ef55e501b22cf", 
             "BoundingBoxes": [
                [[2.680664, 4.586792], [51.127660, 52.008555]],#antwerp region , not returning data
                # [[-4.284668,12.008057],[35.406961,40.847060]] #north africa note min max lat, min max long
                # [[-180, -90], [180, 90]] #world
                ]
             }
   
        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        async for message_json in websocket:
            message = json.loads(message_json)
            message_type = message["MessageType"]
            # metadata=  message["Metadata"]
            if message_type == "PositionReport":
                # the message parameter contains a key of the message type which contains the message itself
                ais_message = message['Message']['PositionReport']
                print(f"[{datetime.now(timezone.utc)}] ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}")
                print("messagrerkrkrkrrkkrkr",message)

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())


