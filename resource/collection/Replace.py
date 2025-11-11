
# REQUIREMENT 7 OPERATION
# IDENTIFIER /req/mf-collection/collection-put

# REQUIREMENT 10
# IDENTIFIER /req/mf-collection/collection-put-success
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import pymeos_initialize, pymeos_finalize, TGeomPoint
from urllib.parse import urlparse, parse_qs


def put_collection(self, collectionId,connection, cursor):

    try:
        content_length = int(self.headers.get('Content-Length', 0))
        put_data = self.rfile.read(content_length)
        data_dict = json.loads(put_data)
        # Get optional If-Match header (ETag)
        if_match = self.headers.get('If-Match')
        cursor.execute(
            "SELECT 1 FROM collections_metadata WHERE id = %s",
            [collectionId]
        )
        exists = cursor.fetchone() 
        if(not exists): 
            if if_match:
                # Requirement 10.E — If-Match header + resource missing = 412
                self.handle_error(412, f"Precondition failed: no collection found with id {collectionId}")
            else:
                # Requirement 10.D No collection found 404
                self.handle_error(404, f"No collection found with id {collectionId}")
            return

        # Entity versioning / ETag support to be added later
        # cursor.execute("SELECT updated_at FROM collections_metadata WHERE id=%s", [collectionId])
        # row = cursor.fetchone()
        # current_etag = f'"{row[0].isoformat()}"'
        # if if_match and if_match != current_etag:
        #     self.handle_error(412, "Precondition failed: ETag mismatch")
        #     return
        if 'itemType' not in data_dict or data_dict['itemType'] is None:
            data_dict['itemType'] = "movingfeature"
        # ignore id if present (requirement 10)
        # NOTE 2:Once set, the update frequency cannot be changed.
        cursor.execute(
            "UPDATE collections_metadata SET title=%s, description=%s, itemtype=%s WHERE id=%s",
            (data_dict.get('title'), data_dict.get('description'),
            data_dict['itemType'], collectionId)
        )
        connection.commit()

        # Rows were updated successfully
        self.send_response_only(204, "OK")
        # self.send_header("ETag", f'"{etag}"')
        self.end_headers()
    except Exception as e:
        print("error ",e)
        self.handle_error(404 if 'does not exist' in str(e) else 500,
                            'no collection was found' if 'does not exist' in str(e) else 'Server internal error')




# The following example replaces the feature created by the Create Example with a new feature (collection metadata without an update frequency). Once again, the replacement feature is represented as a JSON payload. A pseudo-sequence diagram notation is used to illustrate the details of the HTTP communication between the client and the server.
# Client Server | |
# |  PUT /collections/mfc_1 HTTP/1.1                                   |
# |  Content-Type: application/json                                    | 

# | {                                                                  |
# | "title": "MovingFeatureCollection_2",                              |
# | "description": "Title is changed" | | } |
# |------------------------------------------------------------------>|

# | HTTP/1.1 204 OK |
# |<------------------------------------------------------------------|



# EQUIREMENT 10
# IDENTIFIER
# /req/mf-collection/collection-put-success
# INCLUDED IN
# Requirements class 1: http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/req/mf-collection
# A
# A successful execution of the operation SHALL be reported as a response with an HTTP status code200 or 204.
# B
# If the operation is not executed immediately, but is added to a processing queue, the response SHALL have an HTTP status code 202.
# C
# If the representation of the resource submitted in the request body contained a resource identifier, the server SHALL ignore this identifier.
# OPEN GEOSPATIAL CONSORTIUM 22-003R3 35
# REQUIREMENT 10
# D
# If the target resource does not exist and the server does not support creating new resources using PUT, the server SHALL indicate an unsuccessful execution of the operation with an HTTP status code404.
# E
# If the request includes an If-Match header and the resource does not exist, the server SHALL not create a new resource and