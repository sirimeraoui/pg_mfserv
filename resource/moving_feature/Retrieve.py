
#REQ 19
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import pymeos_initialize, pymeos_finalize, TGeomPoint
from urllib.parse import urlparse, parse_qs
import math
from datetime import datetime

hostName = "localhost"
serverPort = 8080

host = 'localhost'
port = 25431
db = 'postgres'
user = 'postgres'
password = 'mysecretpassword'


def get_movement_single_moving_feature(self, collectionId, mFeatureId, connection, cursor):
    try:
        columns = column_discovery(collectionId, cursor)
        id_col = columns[0][0]
        trip_col = columns[1][0]

        sql = f"""
            SELECT {id_col},
                   stbox({trip_col}),
                   extent({trip_col})
            FROM public.{collectionId}
            WHERE {id_col} = %s
        """

        cursor.execute(sql, (mFeatureId,))
        row = cursor.fetchone()

        if row is None:
            self.handle_error(404, "Moving feature not found")
            return

        bbox = row[1]
        time_extent = row[2]

        feature = {
            "type": "Feature",
            "id": str(mFeatureId),
            "geometry": None,
            "properties": {},
            "bbox": bbox,
            "time": time_extent
        }

        send_json_response(self, 200, json.dumps(feature))

    except Exception as e:
        print(e)
        self.handle_error(500, str(e))


























#tgsequence

# def get_movement_single_moving_feature(self, collectionId, mFeatureId,connection, cursor):
#     columns = column_discovery(collectionId, cursor)
#     id = columns[0][0]
#     trip = columns[1][0]
#     try:
#         parsed_url = urlparse(self.path)
#         query_params = parse_qs(parsed_url.query)
#         limit = 10 if query_params.get(
#             'limit') is None else query_params.get('limit')[0]
#         x1, y1, x2, y2 = query_params.get('x1', [None])[0], query_params.get('y1', [None])[0], \
#             query_params.get('x2', [None])[
#             0], query_params.get('y2', [None])[0]
#         if x1 or y1 or x2 or y2 is None:
#             sqlString = f"SELECT {id}, {trip} FROM public.{collectionId} WHERE  {id}={mFeatureId} LIMIT {limit};"
#         else:
#             dateTime = query_params.get('dateTime')
#             dateTime1 = dateTime[0].split(',')[0]
#             dateTime2 = dateTime[0].split(',')[-1]
#             print(dateTime1, dateTime2)
#             sqlString = f"SELECT {id}, asMFJSON({trip}) FROM public.{collectionId} WHERE atstbox({trip}, stbox 'SRID=25832;STBOX XT((({x1},{y1}), ({x2},{y2})),[{dateTime1},{dateTime2}])') IS NOT NULL AND {id}={featureId} LIMIT {limit};"

#         subTrajectory = query_params.get('subTrajectory', [None])[0]
#         leaf = query_params.get('leaf', [None])

#         cursor.execute(sqlString)
#         rs = cursor.fetchall()
#         movements = []
#         for row in rs:
     
#             json_data = row[1].as_mfjson()
#             json_data = json.loads(json_data)
#             movements.append(json_data)

#         full_json = {
#             "type": "TemporalGeometrySequence",
#             "geometrySequence": movements,
#             "timeStamp": "2021-09-01T12:00:00Z",
#             "numberMatched": 100,
#             "numberReturned": 1
#         }
#         json_data = json.dumps(full_json)
#         send_json_response(self, 200, json_data)
#         return full_json

#     except Exception as e:
#         print(str(e))
#         self.handle_error(400, str(e))
