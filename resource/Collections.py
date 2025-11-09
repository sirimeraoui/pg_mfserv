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

def do_collections(self,connection,cursor):
        try:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"
            )
            fetched_collections = cursor.fetchall()

            base_url = f"http://{hostName}:{serverPort}"

            collections_list = []
            for (table_name,) in fetched_collections:
                collections_list.append({
                    "id": table_name,
                    "title": table_name,
                    "links": [
                        {
                            "href": f"{base_url}/collections/{table_name}",
                            "rel": "self",
                            "type": "application/json"
                        }
                    ]
                })

            response = {
                "collections": collections_list,
                "links": [
                    {"href": f"{base_url}/collections", "rel": "self", "type": "application/json"}
                ]
            }

            json_data = json.dumps(response)
            send_json_response(self, 200, json_data)

        except Exception as e:
            self.handle_error(500, f"Internal server error: {str(e)}")

