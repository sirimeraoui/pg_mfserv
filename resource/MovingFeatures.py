
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import *
from urllib.parse import urlparse, parse_qs


hostName = "localhost"
serverPort = 8080

host = 'localhost'
port = 25431
db = 'postgres'
user = 'postgres'
password = 'mysecretpassword'

# CREATE
def do_post_collection_items(self, collectionId, connection, cursor):
    try:
        # Read request body
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        features_list = json.loads(post_data.decode('utf-8'))

        # Wrap single feature into list
        if not isinstance(features_list, list):
            features_list = [features_list]

        new_locations = []
        base_url = f"http://{hostName}:{serverPort}"

        for feature in features_list:
            feat_id = feature.get("id")
            tempGeo = feature.get("temporalGeometry")

            if tempGeo is None:
                print(f"Skipping feature {feat_id}: missing temporalGeometry")
                continue

            try:
                # Store as JSON (OGC MF-JSON representation)
                sql_query = f"""
                    INSERT INTO public.{collectionId} (id, temporalgeometry)
                    VALUES (%s, %s::jsonb)
                """
                cursor.execute(sql_query, (feat_id, json.dumps(tempGeo)))
                connection.commit()

                # Add the feature URI for response
                new_locations.append(f"{base_url}/collections/{collectionId}/items/{feat_id}")

            except Exception as e:
                connection.rollback()
                print(f"Skipping feature {feat_id} due to error: {e}")

        # OGC-compliant response: 201 Created
        self.send_response(201)
        self.send_header("Content-type", "application/geo+json")
        self.end_headers()

        response_body = {
            "type": "FeatureCollection",
            "features": [{"href": uri, "rel": "item"} for uri in new_locations]
        }
        self.wfile.write(bytes(json.dumps(response_body), "utf-8"))

    except Exception as e:
        self.handle_error(400 if "DataError" in str(e) else 500, str(e))

# GET
def do_get_collection_items(self, collectionId,connection, cursor):
    # CHANGES: Added full subTrajectory support including boolean parsing, validation of datetime interval,
    # trimming trajectories, checking for bbox presence, and enforcing leaf exclusion.
    parsed_url = urlparse(self.path)
    query_params = parse_qs(parsed_url.query)
    limit = 10 if query_params.get('limit') is None else int(query_params.get('limit')[0])

    x1 = query_params.get('x1', [None])[0]
    y1 = query_params.get('y1', [None])[0]
    x2 = query_params.get('x2', [None])[0]
    y2 = query_params.get('y2', [None])[0]

    subTrajectory = query_params.get('subTrajectory', [None])[0]
    subTrajectory = True if str(subTrajectory).lower() == 'true' else False

    dateTime = query_params.get('dateTime', [None])[0]

    if subTrajectory:
        if 'leaf' in query_params:
            self.handle_error(400, "subTrajectory cannot be used with leaf parameter")
            return
        if not dateTime or ',' not in dateTime:
            self.handle_error(400, "subTrajectory requires a bounded datetime interval")
            return
        if not (x1 and y1 and x2 and y2):
            self.handle_error(400, "subTrajectory requires bbox parameter")
            return
        dateTime1, dateTime2 = dateTime.split(',')
    else:
        if dateTime:
            dateTime1, dateTime2 = dateTime.split(',')
        else:
            dateTime1 = dateTime2 = None

    columns = column_discovery(collectionId, cursor)
    id_col = columns[0][0]
    trip_col = columns[1][0]

    # Build query for bbox and datetime
    if x1 and y1 and x2 and y2 and dateTime1 and dateTime2:
        query = (
            f"SELECT {id_col}, asMFJSON({trip_col}), count({trip_col}) OVER() as total_count "
            f"FROM public.{collectionId} "
            f"WHERE atstbox({trip_col}, stbox 'SRID=25832;STBOX XT((({x1},{y1}), ({x2},{y2})),[{dateTime1},{dateTime2}])') IS NOT NULL "
            f"LIMIT {limit};"
        )
    else:
        query = f"SELECT {id_col}, asMFJSON({trip_col}), count({trip_col}) OVER() as total_count FROM public.{collectionId} LIMIT {limit};"

    cursor.execute(query)
    data = cursor.fetchall()
    row_count = cursor.rowcount
    total_row_count = data[0][2] if data else 0

    features = []
    for row in data:
        feature_json = json.loads(row[1])
        tPoint = TGeomPoint.from_mfjson(json.dumps(feature_json))

        if subTrajectory:
            tPoint = tPoint.subtrajectory(dateTime1, dateTime2)  # Trim trajectory to interval

        bbox = tPoint.bounding_box()
        feature = json.loads(tPoint.as_mfjson())
        feature["bbox"] = [bbox.xmin(), bbox.ymin(), bbox.xmax(), bbox.ymax()]
        feature["id"] = row[0]
        feature.pop("datetimes", None)
        features.append(feature)

    crs = json.loads(data[0][1])["crs"] if data else None

    geojson_data = {
        "type": "FeatureCollection",
        "features": features,
        "crs": crs,
        "timeStamp": "To be defined",
        "numberMatched": total_row_count,
        "numberReturned": row_count
    }

    send_json_response(self, 200, json.dumps(geojson_data))
