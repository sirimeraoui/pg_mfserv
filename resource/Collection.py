
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
# CREATE
def do_post_collection(self,connection, cursor):
    try:
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data_dict = json.loads(post_data.decode('utf-8'))

        # Create a safe table name from the title
        collection_id = data_dict["title"].lower().replace(" ", "_")

        # Drop existing table (temporary, can ask your teacher later)
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS public.{table}").format(
            table=sql.Identifier(collection_id)
        ))

        # Create table
        cursor.execute(sql.SQL(
            "CREATE TABLE public.{table} (id SERIAL PRIMARY KEY, title TEXT, updateFrequency integer, description TEXT, itemType TEXT)"
        ).format(table=sql.Identifier(collection_id)))

        connection.commit()

        # Base URL for links
        base_url = f"http://{hostName}:{serverPort}"

        # Prepare response body (optional but OGC-compliant)
        response_body = {
            "id": collection_id,
            "title": data_dict.get("title"),
            "description": data_dict.get("description", ""),
            "itemType": data_dict.get("itemType", "movingfeature"),
            "updateFrequency": data_dict.get("updateFrequency", 1000),
            "links": [
                {
                    "href": f"{base_url}/collections/{collection_id}",
                    "rel": "self",
                    "type": "application/json"
                }
            ]
        }

        # Send HTTP 201 Created with Location header
        self.send_response(201)
        self.send_header("Content-type", "application/json")
        self.send_header("Location", f"/collections/{collection_id}")
        self.end_headers()
        self.wfile.write(json.dumps(response_body).encode('utf-8'))

    except Exception as e:
        self.handle_error(500, f"Internal server error: {str(e)}")


# GET
def _collection_id(self, collectionId,connection,cursor):
    try:
        # Verify collection exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (collectionId,))
        
        if not cursor.fetchone()[0]:
            self.handle_error(404, 'Collection not found')
            return

        base_url = f"http://{hostName}:{serverPort}"

        # Fetch features to compute extent
        cursor.execute(f"SELECT * FROM public.{collectionId}")
        rows = cursor.fetchall()

        # Default CRS
        crs_list = ["https://www.opengis.net/def/crs/OGC/1.3/CRS84"]

        # Compute spatial bbox
        bbox = None
        temporal_interval = None
        if rows:
            min_x = min_y = float('inf')
            max_x = max_y = float('-inf')
            min_time = None
            max_time = None

            columns = column_discovery(collectionId, cursor)
            id_col = columns[0][0]
            geom_col = columns[1][0]

            for row in rows:
                mf_json = json.loads(row[columns[1][0]]) if isinstance(row[columns[1][0]], str) else json.loads(row[1].as_mfjson())
                tPoint = TGeomPoint.from_mfjson(json.dumps(mf_json))
                feature_bbox = tPoint.bounding_box()
                min_x = min(min_x, feature_bbox.xmin())
                min_y = min(min_y, feature_bbox.ymin())
                max_x = max(max_x, feature_bbox.xmax())
                max_y = max(max_y, feature_bbox.ymax())

                # Temporal extent
                times = []
                if "datetimes" in mf_json:
                    times = mf_json["datetimes"]
                elif "coordinates" in mf_json:
                    times = [c[2] for c in mf_json["coordinates"] if len(c) >= 3]  # assuming time in 3rd position
                if times:
                    min_time_feature = min(times)
                    max_time_feature = max(times)
                    min_time = min(min_time, min_time_feature) if min_time else min_time_feature
                    max_time = max(max_time, max_time_feature) if max_time else max_time_feature

            bbox = [min_x, min_y, max_x, max_y]
            temporal_interval = [min_time, max_time] if min_time and max_time else None

        # Build metadata response
        collection_metadata = {
            "id": collectionId,
            "title": collectionId,
            "description": f"Collection of moving features: {collectionId}",
            "links": [
                {
                    "href": f"{base_url}/collections/{collectionId}",
                    "rel": "self",
                    "type": "application/json",
                    "title": "This document"
                },
                {
                    "href": f"{base_url}/collections/{collectionId}/items",
                    "rel": "items",
                    "type": "application/geo+json",
                    "title": "Moving features as GeoJSON"
                }
            ],
            "itemType": "movingfeature",
            "extent": {
                "spatial": {
                    "bbox": bbox if bbox else [-180, -90, 180, 90],
                    "crs": crs_list
                },
                "temporal": {
                    "interval": temporal_interval if temporal_interval else [],
                    "trs": ["http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"]
                }
            },
            "crs": crs_list,
            "updateFrequency": 1000  # example fixed value, can be dynamic
        }

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(collection_metadata).encode('utf-8'))

    except Exception as e:
        self.handle_error(500, str(e))

#DELETE
def do_delete_collection(self,collectionId,connection, cursor):

        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (collectionId,))
            exists = cursor.fetchone()[0]

            if not exists:
                # 404  Not found
                self.handle_error(404, f"Collection '{collectionId}' not found.")
                return

            # Drop
            cursor.execute(sql.SQL("DROP TABLE public.{table}")
                        .format(table=sql.Identifier(collectionId)))
            connection.commit()

            # 404 per OCG
            self.send_response(204)
            self.send_header("Content-type", "application/json")
            self.end_headers()

        except Exception as e:
            self.handle_error(500, f"Internal server error: {str(e)}")

