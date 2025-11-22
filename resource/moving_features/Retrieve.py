
# REQUIREMENT 14
# IDENTIFIER
# /req/movingfeatures/features-get
from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import pymeos_initialize, pymeos_finalize, TGeomPoint
from urllib.parse import urlparse, parse_qs
import math
from datetime import datetime

hostName = "localhost"
serverPort = 8080

host = 'localhost'
port = 25431
db = 'postgres'
user = 'postgres'
password = 'mysecretpassword'


def get_collection_items(self, collectionId, connection, cursor):
    from dateutil import parser, tz
    try:
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        # get prams
        try:
            limit = min(int(query_params.get('limit', [10])[0]), 1000)
        except ValueError:
            return self.handle_error(400, "Invalid limit parameter")

        bbox = query_params.get('bbox', [None])[0]
        datetime_param = query_params.get('datetime', [None])[0]
        subTrajectory = query_params.get('subTrajectory', ['false'])[
            0].lower() == "true"
        leaf = query_params.get('leaf', ['false'])[0].lower() == "true"

        # parse bbox
        x1 = y1 = x2 = y2 = None
        if bbox:
            try:
                coords = [float(c) for c in bbox.split(',')]
                if len(coords) != 4:
                    return self.handle_error(400, "Invalid bbox format")
                x1, y1, x2, y2 = coords
            except Exception:
                return self.handle_error(400, "Invalid bbox coordinates")

        # parse datetime
        dt1 = dt2 = None
        if datetime_param:
            if "/" in datetime_param:
                dt1, dt2 = datetime_param.split("/")
            else:
                dt1 = datetime_param  # instant

        if subTrajectory and leaf:
            return self.handle_error(400, "subTrajectory cannot be used with leaf")
        if subTrajectory and not (dt1 and dt2):
            return self.handle_error(400, "subTrajectory requires a datetime interval")

        columns = column_discovery(collectionId, cursor)
        if not columns or len(columns) < 2:
            return self.handle_error(404, f"Collection {collectionId} not found")

        trip_col = columns[0][0]   # trajectory column
        id_col = columns[1][0]     # id column

    # where
        filters = []

        if x1 is not None and dt1 and dt2:
            filters.append(
                f"atstbox({trip_col}, stbox 'STBOX XT((({x1},{y1}),({x2},{y2})),[{dt1},{dt2}])', false) IS NOT NULL"
            )
        elif x1 is not None:
            filters.append(
                f"atstbox({trip_col}, stbox 'STBOX X(({x1},{y1}),({x2},{y2}))', false) IS NOT NULL"
            )
        elif dt1 and dt2:
            filters.append(
                f"atstbox({trip_col}, stbox 'STBOX T([{dt1},{dt2}])', false) IS NOT NULL"
            )

        where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

    #    main
        query = f"""
            SELECT {id_col}, asMFJSON({trip_col}), 
                   COUNT(*) OVER() AS total_count
            FROM public.{collectionId}
            {where_clause}
            LIMIT {limit};
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            return send_json_response(self, 200, json.dumps({
                "type": "FeatureCollection",
                "features": [],
                "timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "numberMatched": 0,
                "numberReturned": 0,
                "links": [{"href": self.path, "rel": "self", "type": "application/geo+json"}]
            }))

        total_count = rows[0][2]
        features = []

        # features construction
        for row in rows:
            feature_id = row[0]
            mf_json = json.loads(row[1]) if row[1] else None
            if not mf_json:
                continue

            # subTrajectory
            if subTrajectory:
                dt1_sql = dt1.replace("T", " ")
                dt2_sql = dt2.replace("T", " ")
                cursor.execute(
                    f"""
                        SELECT asMFJSON(
                            atstbox({trip_col}, stbox 'STBOX T([{dt1_sql},{dt2_sql}])')
                        )
                        FROM public.{collectionId}
                        WHERE {id_col} = %s;
                    """,
                    [feature_id]
                )
                sub = cursor.fetchone()
                if sub and sub[0]:
                    mf_json = json.loads(sub[0])

            # leaf → last p -instnt
            if leaf and "datetimes" in mf_json:
                mf_json["coordinates"] = [mf_json["coordinates"][-1]]
                mf_json["datetimes"] = [mf_json["datetimes"][-1]]

            # Normalize datetimes to match input timezone
            if "datetimes" in mf_json:
                new_dts = []
                for dt in mf_json["datetimes"]:
                    dt_obj = parser.isoparse(dt)
                    # Convert to requested tz if dt1 exists
                    if dt1:
                        tzinfo = tz.gettz(dt1[-3:])  # +01
                        dt_obj = dt_obj.astimezone(tzinfo)
                    new_dts.append(dt_obj.isoformat())
                mf_json["datetimes"] = new_dts

            feature = {
                "type": "Feature",
                "id": feature_id,
                "temporalGeometry": mf_json
            }
            features.append(feature)

        response = {
            "type": "FeatureCollection",
            "features": features,
            "timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "numberMatched": total_count,
            "numberReturned": len(features),
            "links": [{"href": self.path, "rel": "self", "type": "application/geo+json"}]
        }

        send_json_response(self, 200, json.dumps(response))

    except Exception as e:
        print("ERROR:", e)
        self.handle_error(500, f"Internal server error: {str(e)}")
