
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
    try:
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # limit
        try:
            limit = min(int(query_params.get('limit', [10])[0]), 1000)
        except ValueError:
            self.handle_error(400, "Invalid limit parameter")
            return

        # BBOX
        bbox = query_params.get('bbox', [None])[0]
        x1 = y1 = x2 = y2 = None
        if bbox:
            try:
                coords = [float(c) for c in bbox.split(',')]
                if len(coords) != 4:
                    self.handle_error(400, "Invalid bbox format")
                    return
                x1, y1, x2, y2 = coords
            except ValueError:
                self.handle_error(400, "Invalid bbox coordinates")
                return

        #  datetime
        datetime_param = query_params.get('datetime', [None])[0]
        dateTime1 = dateTime2 = None
        if datetime_param:
            if '/' in datetime_param:
                dateTime1, dateTime2 = datetime_param.split('/')
            else:
                dateTime1 = datetime_param

        #  subTrajectory
        subTrajectory = query_params.get('subTrajectory', ['false'])[
            0].lower() == 'true'

        # leaf
        leaf = query_params.get('leaf', ['false'])[0].lower() == 'true'

        # Validate parameter conflicts
        if subTrajectory and leaf:
            self.handle_error(
                400, "subTrajectory parameter cannot be used with leaf parameter")
            return
        if subTrajectory and not datetime_param:
            self.handle_error(
                400, "subTrajectory parameter requires datetime parameter")
            return
# get cols
        columns = column_discovery(collectionId, cursor)
        if not columns or len(columns) < 2:
            self.handle_error(404, f"Collection {collectionId} not found")
            return

        id_col = columns[0][0]
        trip_col = columns[1][0]

        # Build query using atstbox if bbox or datetime
        query_parts = []
        params = []

        if x1 is not None and y1 is not None and dateTime1 and dateTime2:
            query_parts.append(
                f"atstbox({trip_col}, STBOX 'SRID=4326;STBOX XT(({x1},{y1}),({x2},{y2})),[{dateTime1},{dateTime2}])') IS NOT NULL"
            )
        elif x1 is not None and y1 is not None:
            query_parts.append(
                f"atstbox({trip_col}, STBOX 'SRID=4326;STBOX XY(({x1},{y1}),({x2},{y2}))') IS NOT NULL"
            )
        elif dateTime1 and dateTime2:
            query_parts.append(
                f"atstbox({trip_col}, STBOX 'SRID=4326;STBOX T([{dateTime1},{dateTime2}])') IS NOT NULL"
            )

        where_clause = " AND ".join(query_parts)
        if where_clause:
            where_clause = "WHERE " + where_clause

        query = f"""
            SELECT {id_col}, asMFJSON({trip_col}), count({trip_col}) OVER() as total_count
            FROM public.{collectionId}
            {where_clause}
            LIMIT {limit};
        """

        cursor.execute(query)
        data = cursor.fetchall()
        if not data:
            send_json_response(self, 200, json.dumps({
                "type": "FeatureCollection",
                "features": [],
                "timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "numberMatched": 0,
                "numberReturned": 0,
                "links": [{"href": self.path, "rel": "self", "type": "application/geo+json"}]
            }))
            return

        total_row_count = data[0][2]
        features = []

        for row in data:
            feature_id = row[0]
            mf_json_str = row[1]

            if subTrajectory and dateTime1 and dateTime2:
                cursor.execute(
                    f"SELECT asMFJSON(atPeriod({trip_col}, '[{dateTime1},{dateTime2}]')) "
                    f"FROM public.{collectionId} WHERE {id_col} = %s", [
                        feature_id]
                )
                sub_res = cursor.fetchone()
                if sub_res and sub_res[0]:
                    mf_json_str = sub_res[0]

            if mf_json_str:
                feature = json.loads(mf_json_str)
                feature["id"] = feature_id

                # Leaf: only return last instant of trajectory
                if leaf and "datetimes" in feature:
                    feature["coordinates"] = [feature["coordinates"][-1]]
                    feature["datetimes"] = [feature["datetimes"][-1]]

                features.append(feature)

        response_data = {
            "type": "FeatureCollection",
            "features": features,
            "timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "numberMatched": total_row_count,
            "numberReturned": len(features),
            "links": [{"href": self.path, "rel": "self", "type": "application/geo+json"}],
        }

        send_json_response(self, 200, json.dumps(response_data))

    except Exception as e:
        self.handle_error(500, f"Internal server error: {str(e)}")
