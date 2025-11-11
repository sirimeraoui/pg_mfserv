# REQUIREMENT 6: OPERATION 
#IDENTIFIER /req/mf-collection/collection-get
# REQUIREMENT 9: RESPONSE
# The response SHALL only include collection metadata selected by the request.
#   "extent": { 
#                         "spatial": { 
#                             "bbox": [ -180, -90, 190, 90 ],
#                             "crs": "https://www.opengis.net/def/crs/OGC/1.3/CRS84"  #default
#                             }, 
#                         "temporal": {
#                             "interval": [ "2011-11-11T12:22:11Z","2012-11-24T12:32:43Z" ],
#                             "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
#                             }}
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


def get_collection_id(self, collectionId,connection,cursor):
    try:
        cursor.execute(
            "SELECT * FROM collections_metadata WHERE id = %s;",
            [collectionId],
        )
        r = cursor.fetchone()
        if(r): columns = [desc[0] for desc in cursor.description]
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = %s
            );
            """,
            [collectionId],
        )
        
        table_exists = cursor.fetchone()[0]
        exists = r and table_exists

        if(not exists):
            self.handle_error(404, f"No collection found with id {collectionId}")
            return

        # Convert rows to dicts
        collection= dict(zip(columns, r))
        print("the dictionar of collection is: ", collection)
        # Convert fetched data to JSON
        res = {
            "id": collection["id"],
            "title": collection["title"],
            "itemType": collection["itemtype"],
            "updateFrequency": collection["updatefrequency"],
            # extent is hardcoded, default crs and trs, to be updated
            "extent": { 
                "spatial": { 
                    "bbox": [ -180, -90, 190, 90 ],
                    "crs": ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]
                    }, 
                "temporal": {
                    "interval": [ "2011-11-11T12:22:11Z","2012-11-24T12:32:43Z" ],
                    "trs": ["http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"]
                    }},
            "links": [ { "href": f"https://data.example.org/collections/{collectionId}", "rel": "self", "type": "application/json" } ]
        }
        query_params = parse_qs(urlparse(self.path).query)
        print("qeuryr params ", query_params)
        print('fjffjfj')
        fields_param = query_params.get("fields", [None])[0]
        required_fields = ["id", "itemType", "links"]
        
        if fields_param:
            # request selected specific fields as per requirement 9
            requested_fields = fields_param.split(",")
            # always include required fields
            filtered_res = {k: v for k, v in res.items() if k in requested_fields or k in required_fields}
        else:
            # No fields specified, return full response
            filtered_res = res

        res = json.dumps(filtered_res)

        # Send response
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(res.encode('utf-8'))
    except Exception as e:
        # Handle any exceptions
        print("error",e)
        self.handle_error(404 if 'does not exist' in str(e) else 500,
                            'no collection was found' if 'does not exist' in str(e) else 'Server internal error')

# eg.JSON_payload = {
#     "id": "mfc-1",
#     "title": "moving_feature_collection_sample",
#     "itemType": "movingfeature",
#     "updateFrequency": 1000,
#     "extent": { 
#         "spatial": { 
#             "bbox": [ -180, -90, 190, 90 ], 
#             "crs": [ "http://www.opengis.net/def/crs/OGC/1.3/CRS84" ] },
#         "temporal": { 
#             "interval": [ "2011-11-11T12:22:11Z","2012-11-24T12:32:43Z" ],
#             "trs": [ "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian" ] } }, 
#     "links": [ { "href": "https://data.example.org/collections/mfc-1", "rel": "self", "type": "application/json" } ]
#     }
# Listing 8 — An Example of Collection GET Operation: