# REQUIREMENT 2
# /req/mf-collection/collections-post

from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import pymeos_initialize, pymeos_finalize, TGeomPoint
from urllib.parse import urlparse, parse_qs


hostName = "localhost"
serverPort = 8080

host = 'localhost'
port = 25431
db = 'postgres'
user = 'postgres'
password = 'mysecretpassword'

# 
def post_collections(self,connection,cursor):
    try:
        content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        # print("POST request,\nPath: %s\nHeaders: %s\n\nBody: %s\n" % (
        #     self.path, self.headers, post_data.decode('utf-8')))
        data_dict = json.loads(post_data.decode('utf-8'))
    #     expected_types = {
    #         "title": str,
    #         "updateFrequency": int,
    #         "description": str,
    #         "itemType": str
    #    }

    #     # Check each field type
    #     for expected_type in expected_types.items():
    #         if not isinstance(data_dict[key], expected_type):
    #             raise TypeError(f"Field '{key}' must be of type {expected_type.__name__}, "
    #                             f"but got {type(data_dict[key]).__name__}")
        if "itemType" not in data_dict or not data_dict["itemType"]:
            data_dict["itemType"] = "movingfeature"
        # print("All fields have correct types")
        cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS collections_metadata (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    updateFrequency INTEGER,
                    description TEXT,
                    itemType TEXT) """))
        connection.commit()
        table_name = data_dict["title"].lower()
        cursor.execute("SELECT id FROM collections_metadata WHERE id = %s", ( table_name.replace("'", "")  ,))
            
        exists = cursor.fetchone()
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema='public' AND table_name=%s
            )
        """, (table_name,))
        table_exists = cursor.fetchone()[0]
        exists = exists and table_exists

        if(exists):
            self.handle_error(409, f'Collection {data_dict["title"]} already exists.')
            return   
        else: 
            cursor.execute("INSERT INTO  collections_metadata VALUES(%s, %s,%s,%s,%s)",(data_dict["title"].lower(),data_dict["title"].lower(), data_dict["updateFrequency"], data_dict["description"], data_dict["itemType"]))
            cursor.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {} (id TEXT PRIMARY KEY)").format(
                    sql.Identifier(table_name)
                )
            )

            connection.commit()

            # If the operation is not executed immediately, but is added to a processing queue, the response SHALL have an HTTP status code 202.
            self.send_response(201)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(post_data.decode('utf-8'), "utf-8"))
    except Exception as e:
        print("message:", e)
        self.handle_error(500, 'Internal server error')
