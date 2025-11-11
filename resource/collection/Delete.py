
# REQUIREMENT 8 OPERATION
# IDENTIFIER /req/mf-collection/collection-delete
# REQUIREMENT 11
# IDENTIFIER
# /req/mf-collection/collection-delete-success
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
def delete_collection(self, collectionId,connection, cursor):
    try:
        cursor.execute(
            "SELECT 1 FROM collections_metadata WHERE id = %s",
            [collectionId]
        )
        metadata_exists = cursor.fetchone() is not None

        # Check if table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            )
            """,
            [collectionId]
        )
        table_exists = cursor.fetchone()[0]
        exists = metadata_exists and table_exists
        if(not exists):
            self.handle_error(404, f"No collection found with id {collectionId}")
            return

# % removed: cursor.execute("DROP TABLE IF EXISTS public.%s" % collectionId) avoid sql injection
        cursor.execute(
            "DELETE FROM collections_metadata WHERE id = %s",
            [collectionId]
        )
        cursor.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(collectionId))
        )

        connection.commit()
        self.send_response_only(204, "OK")
        self.end_headers()
    except Exception as e:
        # print("eee",e)
        self.handle_error(500, str(e))


# Client Server | | | DELETE /collections/mfc_1 HTTP/1.1 | 
# |------------------------------------------------------------------>| 
# |                                                                   |
# | HTTP/1.1 204 OK                                                   |
# |<------------------------------------------------------------------|
# Listing 6 — An Example of Deleting an Existing Collection:



# REQUIREMENT 11
# IDENTIFIER
# /req/mf-collection/collection-delete-success
# INCLUDED IN
# Requirements class 1: http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/req/mf-collection
# A
# A successful execution of the operation SHALL be reported as a response with an HTTP status code200 or 204.
# B
# If the operation is not executed immediately, but is added to a processing queue, the response SHALL have an HTTP status code 202.
# C
# If no resource with the identifier exists in the collection, the server SHALL respond with a not-found exception (404).