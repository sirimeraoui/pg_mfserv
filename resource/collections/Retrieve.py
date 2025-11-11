# REQUIREMENT 1 OPERATION
# IDENTIFIER /req/mf-collection/collections-get
# REQUIREMENT 3 RESPONSE
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
            cursor.execute("SELECT * FROM collections_metadata;")
            fetched_collections = cursor.fetchall()
            base_url = f"http://{hostName}:{serverPort}"
            columns = [desc[0] for desc in cursor.description]
            # Convert rows to dicts
            collections = [dict(zip(columns, row)) for row in fetched_collections ]
            # print("collections dict", collections)
            collections_list = []
            for _ in collections:
                collections_list.append({
                    "id": _["id"],
                    "title": _["title"],
                    "updateFrequency":_["updatefrequency"],
                    "description":_["description"],
                    "itemType":_["itemtype"],
                    "extent": { 
                        "spatial": { 
                            "bbox": [ -180, -90, 190, 90 ],
                            "crs": ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]
                            }, 
                        "temporal": {
                            "interval": [ "2011-11-11T12:22:11Z","2012-11-24T12:32:43Z" ],
                            "trs": ["http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"]
                            }},
                    "links": [
                        {"href": f"{base_url}/collections/{ _["title"]}","rel": "self","type": "application/json"}
                        #, {"href": "http://localhost:8080/collections/<collection id>/schema","rel": "[ogc-rel:schema]","type": "application/schema+json"}
                        ]
                })

            response = {
                "links": [
                    {"href": f"{base_url}/collections", "rel": "self", "type": "application/json"}
                    #, {"href": "http://localhost:8080/collections.html", "rel": "alternate","type": "text/html"}
                ],
                "collections": collections_list,
            }
            json_data = json.dumps(response)
            send_json_response(self, 200, json_data)

        except Exception as e:
            # print("error: ", e)
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


# example:
# Json_payload = { "collections": 
# [ 
#     { "id": "mfc-1", 
#     "title": "MovingFeatureCollection_1", 
#     "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
#      "itemType": "movingfeature", 
#      "updateFrequency": 1000, 
#      "extent": { "spatial":{
#         "bbox": [ -180, -90, 190, 90 ], 
#         "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84" }, 
#        "temporal": { 
#         "interval": [ "2011-11-11T12:22:11Z","2012-11-24T12:32:43Z" ],
#         "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian" } },
# "links": [ { "href": "https://data.example.org/collections/mfc-1", "rel": "self", "type": "application/json" } ] } ], "links": [ { "href": "https://data.example.org/collections", "rel": "self", "type": "application/json" } ]}
# Listing 4 — An Example of a Collections JSON Payload: