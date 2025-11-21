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
from pymeos import pymeos_initialize, pymeos_finalize, TGeomPoint, Temporal
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

        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode("utf-8"))

        object_type = data.get("type")
        if not object_type:
            raise Exception("DataError: Missing mandatory 'type'")
#
        cursor.execute(
            "SELECT 1 FROM pg_tables WHERE tablename=%s;", (collectionId,)
        )
        if cursor.fetchone() is None:
            raise Exception("DataError: collection does not exist")

        created_feature_ids = []   # <-- list of all new features created

        if object_type == "FeatureCollection":
            features = data.get("features")
            if not isinstance(features, list):
                raise Exception(
                    "DataError: FeatureCollection missing 'features' array")

            for feat in features:
                new_id = self.insert_feature(
                    feat, collectionId, connection, cursor)
                created_feature_ids.append(new_id)

        elif object_type == "Feature":
            new_id = self.insert_feature(
                data, collectionId, connection, cursor)
            created_feature_ids.append(new_id)

        else:
            raise Exception("DataError: Invalid 'type'")

        connection.commit()

        self.send_response(201)
        self.send_header("Content-Type", "application/geo+json")

        for fid in created_feature_ids:
            loc = f"/collections/{collectionId}/items/{fid}"
            self.send_header("Location", loc)

        self.end_headers()

        # self.wfile.write(post_data)

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

    if feature.get("type") != "Feature":
        raise Exception("DataError: Invalid feature type")

    feat_id = feature.get("id")
    if feat_id is None:
        feat_id = str(uuid.uuid4())
    else:
        feat_id = str(feat_id)

    temporalGeometry = feature.get("temporalGeometry")
    tGeomPoint = None
    if temporalGeometry:
        if isinstance(temporalGeometry, dict):
            tGeomPoint = TGeomPoint.from_mfjson(json.dumps(temporalGeometry))
        else:
            tGeomPoint = TGeomPoint.from_mfjson(temporalGeometry)

    # check existing columns
    cursor.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=%s;",
        (collectionId,),
    )
    existing_cols = {row[0].lower() for row in cursor.fetchall()}

    # add columns if missing
    if "temporalgeometry" not in existing_cols and tGeomPoint is not None:
        cursor.execute(
            sql.SQL(
                "ALTER TABLE {} ADD COLUMN temporalgeometry public.tgeompoint;"
            ).format(sql.Identifier(collectionId))
        )
        connection.commit()

    # --------- Insert the feature with conflict check ---------
    cursor.execute(
        sql.SQL(
            "INSERT INTO {} (id, temporalgeometry) VALUES (%s, %s::tgeompoint) "
            "ON CONFLICT (id) DO NOTHING RETURNING id;"
        ).format(sql.Identifier(collectionId)),
        (feat_id, str(tGeomPoint))
    )
    inserted = cursor.fetchone()
    if inserted:
        print(f"Inserted feature {feat_id}")
    else:
        print(f"Feature {feat_id} already exists, skipped")

    return feat_id
