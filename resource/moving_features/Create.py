# REQUIREMENT 15
# IDENTIFIER
# /req/movingfeatures/features-post

import uuid
from datetime import datetime

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


def post_collection_items(self, collectionId, connection, cursor):
    try:
        # ---------------- READ BODY ----------------
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode("utf-8"))

        object_type = data.get("type")
        if not object_type:
            raise Exception("DataError: Missing mandatory 'type'")

        # ---------------- Ensure table exists ----------------
        cursor.execute(
            "SELECT 1 FROM pg_tables WHERE tablename=%s;", (collectionId,)
        )
        if cursor.fetchone() is None:
            raise Exception("DataError: collection does not exist")

        # ---------------- Process FeatureCollection ----------------
        if object_type == "FeatureCollection":
            features = data.get("features")
            if not isinstance(features, list):
                raise Exception(
                    "DataError: FeatureCollection missing 'features' array")
            for feat in features:
                self.insert_feature(feat, collectionId, connection, cursor)

        # ---------------- Process single Feature ----------------
        elif object_type == "Feature":
            self.insert_feature(data, collectionId, connection, cursor)

        else:
            raise Exception("DataError: Invalid 'type'")

        connection.commit()
        self.send_response(201)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    except Exception as e:
        msg = str(e)
        if "DataError" in msg:
            code = 400
        elif "does not exist" in msg:
            code = 404
        elif "ConflictError" in msg:
            code = 409
        else:
            code = 500
        print(msg)
        self.handle_error(code, msg)


def insert_feature(self, feature, collectionId, connection, cursor):
    # --------- Validate mandatory fields ---------
    if feature.get("type") != "Feature":
        raise Exception("DataError: Invalid feature type")

    # --------- Handle ID ---------
    feat_id = feature.get("id")
    if feat_id is None:
        feat_id = str(uuid.uuid4())  # auto-generate
    else:
        feat_id = str(feat_id)  # enforce TEXT type

    # --------- Handle temporalGeometry ---------
    temporalGeometry = feature.get("temporalGeometry")
    tGeomPoint = None
    if temporalGeometry:
        tGeomPoint = TGeomPoint.from_mfjson(json.dumps(temporalGeometry))
        print(tGeomPoint)

    # --------- Check existing columns ---------
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=%s;",
        (collectionId,),
    )
    existing_cols = {row[0].lower() for row in cursor.fetchall()}

    # --------- Add missing columns ---------
    # if "id" not in existing_cols:
    #     cursor.execute(
    #         sql.SQL("ALTER TABLE {} ADD COLUMN id TEXT PRIMARY KEY;").format(sql.Identifier(collectionId))
    #     )

    if "temporalgeometry" not in existing_cols and tGeomPoint is not None:
        cursor.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN temporalgeometry tgeompoint;").format(
                sql.Identifier(collectionId))
        )
        connection.commit()

    # --------- Insert the feature with conflict check ---------
    try:
        cursor.execute(
            sql.SQL("INSERT INTO {} (id, temporalgeometry) VALUES (%s, %s);")
            .format(sql.Identifier(collectionId)),
            (feat_id, tGeomPoint)
        )
    except Exception as e:
        # Handle duplicate primary key
        if "duplicate key value violates unique constraint" in str(e):
            raise Exception(
                f"ConflictError: Feature with id '{feat_id}' already exists")
        else:
            raise


# Feature:https://docs.ogc.org/is/19-045r3/19-045r3.html#mfeature
# {
#   "type": "Feature", //(MANDATORY)
#   "temporalGeometry": {...}, //(MANDATORY)
#   "temporalProperties": [...], //(OPTIONAL)
#   "crs" : {...}, //(DEFAULT)
#   "trs" : {...}, //(DEFAULT)
#   "time": [...], //(OPTIONAL)
#   "bbox": [...], //(OPTIONAL)
#   "geometry": {...},  //(OPTIONAL)
#   "properties": {...}, //(OPTIONAL)
#   "id": ... //(OPTIONAL)
# }

# Feature collection: https://docs.ogc.org/is/19-045r3/19-045r3.html#mfeaturecollection
# {
#   "type": "FeatureCollection", //(MANDATORY)
#   "features": [...], //(MANDATORY)
#   "crs" : {...}, //(DEFAULT)
#   "trs" : {...}, //(DEFAULT)
#   "bbox": [...], //(OPTIONAL)
#   "time": [...], //(OPTIONAL)
#   "label": "..."  //(OPTIONAL)
# }
