
#REQ 26
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


def post_tgsequence(self, connection, cursor):
    collection_id = self.path.split('/')[2]
    feature_id = self.path.split('/')[4]
    self.do_add_movement_data_in_mf(collection_id, feature_id)


def add_movement_data_in_mf(self, collectionId, featureId, connection, cursor):
        columns = column_discovery(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]

        try:
            print("POST request,\nPath: %s\nHeaders: %s\n" %
                  (self.path, self.headers))
            # <--- Gets the size of data
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data_dict = json.loads(post_data.decode('utf-8'))

            print(data_dict)
            tgeompoint = TGeomPoint.from_mfjson(json.dumps(data_dict))

            sqlString = f"UPDATE public.{collectionId} SET {trip}= merge({trip}, '{tgeompoint}') where {id} = {featureId}"
            cursor.execute(sqlString)
            connection.commit()

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
        except Exception as e:
            self.handle_error(400, str(e))