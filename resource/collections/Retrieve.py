# REQUIREMENT 1
# IDENTIFIER /req/mf-collection/collections-get

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



def get_collections(self,connection,cursor):
        try:
            cursor.execute(
                "SELECT tablee FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"
            )
            fetched_collections = cursor.fetchall()
            print("resutlssssss:")
            print(fetched_collections)
            base_url = f"http://{hostName}:{serverPort}"

            # collections_list = []
            # for (table_name,) in fetched_collections:
            #     collections_list.append({
            #         "id": table_name,
            #         "title": table_name,
            #         "links": [
            #             {
            #                 "href": f"{base_url}/collections/{table_name}",
            #                 "rel": "self",
            #                 "type": "application/json"
            #             }
            #         ]
            #     })

            # response = {
            #     "links": [
            #         {"href": f"{base_url}/collections", "rel": "self", "type": "application/json"}
            #     ],
            #     "collections": collections_list,
            # }
            json_data = json.dumps({"hello":"world"})
            # json_data = json.dumps(response)
            send_json_response(self, 200, json_data)

        except Exception as e:
            self.handle_error(500, f"Internal server error: {str(e)}")

# based on the following JSON Schema 
# https://docs.ogc.org/DRAFTS/20-024.html#req_collections_collections-list-success
# https://github.com/opengeospatial/ogcapi-common/blob/master/collections/openapi/schemas/common-geodata/collections.yaml
# type: object
# required:
#   - links
#   - collections
# properties:
#   links:
#     type: array
#     title: Links to resource in the collections
#     description: Links to this or other resources provided by the collections.
#     items:
#       $ref: '../common-core/link.yaml'
#   numberMatched:
#     $ref: 'numberMatched.yaml'
#   numberReturned:
#     $ref: 'numberReturned.yaml'
#   collections:
#     type: array
#     title: Collections descriptions
#     description: Descriptions of each collection in this API. 
#     items:
#       $ref: 'collectionDesc.yaml'



# {
#   "links": [
#     {
#       "href": "http://localhost:8080/collections",
#       "rel": "self",
#       "type": "application/json"
#     },
#     {
#       "href": "http://localhost:8080/collections.html",
#       "rel": "alternate",
#       "type": "text/html"
#     }
#   ],
#   "collections": [
#     {
#       "id": "<collection id>",
#       "title": "<collection title>",
        # "updateFrequency": 1000,
#       "description": "<collection description>",
#       "itemType": "<itemType>",
#       "links": [
#         {
#           "href": "http://localhost:8080/collections/<collection id>",
#           "rel": "self",
#           "type": "application/json"
#         },
#         {
#           "href": "http://localhost:8080/collections/<collection id>/schema",
#           "rel": "[ogc-rel:schema]",
#           "type": "application/schema+json"
#         }
#       ]
#     }
#   ]
# }

