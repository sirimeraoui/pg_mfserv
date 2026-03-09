
#REQ 20
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



def delete_single_moving_feature(self, collectionId, mFeature_id, connection, cursor):

    columns = column_discovery(collectionId, cursor)
    id_col = columns[0][0]

    try:
        sql = f"DELETE FROM public.{collectionId} WHERE {id_col} = %s"
        cursor.execute(sql, (mFeature_id,))
        connection.commit()

        if cursor.rowcount == 0:
            self.handle_error(404, "Feature not found")
            return

        self.send_response(204)
        self.end_headers()

    except Exception as e:
        self.handle_error(500, str(e))


























# def delete_single_moving_feature(self, collectionId, mfeature_id, connection, cursor):
#     columns = column_discovery(collectionId, cursor)
#     id = columns[0][0]
#     try:
#         print("GET request,\nPath: %s\nHeaders: %s\n" %
#                 (self.path, self.headers))
#         sqlString = f"DELETE FROM public.{collectionId} WHERE {id}={mfeature_id}"
#         cursor.execute(sqlString)
#         connection.commit()

#         self.send_response(204)
#         self.send_header("Content-type", "application/json")
#         self.end_headers()
#     except Exception as e:
#         self.handle_error(404 if "does not exist" in str(e) else 500,
#                             "Collection or Item does not exist" if "does not exist" in str(
#                                 e) else "Server Internal Error")

