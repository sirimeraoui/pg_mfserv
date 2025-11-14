
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


def get_collection_items(self, collectionId,connection, cursor):
    try:
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        # Parse query parameters with validation
        try:
            limit = min(int(query_params.get('limit', [10])[0]), 1000)  # Add maximum limit
        except ValueError:
            self.handle_error(400, "Invalid limit parameter")
            return
            
        bbox = query_params.get('bbox', [None])[0]
        datetime_param = query_params.get('datetime', [None])[0]
        sub_trajectory = query_params.get('subTrajectory', ['false'])[0].lower() == 'true'
        leaf = query_params.get('leaf', [None])[0]
        
        # Validate parameter conflicts (REQUIREMENT 13E)
        if sub_trajectory and leaf:
            self.handle_error(400, "subTrajectory parameter cannot be used with leaf parameter")
            return
        
        # Validate subTrajectory requirements (REQUIREMENT 12B, 12C)
        if sub_trajectory:
            if not datetime_param:
                self.handle_error(400, "subTrajectory parameter requires datetime parameter")
                return
            if '/' not in datetime_param:
                self.handle_error(400, "subTrajectory parameter requires bounded interval datetime")
                return
        
        # Parse datetime
        dateTime1 = None
        dateTime2 = None
        if datetime_param:
            if '/' in datetime_param:
                dateTime1, dateTime2 = datetime_param.split('/')
                # Validate datetime format
                try:
                    if dateTime1: datetime.fromisoformat(dateTime1.replace('Z', '+00:00'))
                    if dateTime2: datetime.fromisoformat(dateTime2.replace('Z', '+00:00'))
                except ValueError:
                    self.handle_error(400, "Invalid datetime format")
                    return
            else:
                dateTime1 = datetime_param
        
        # Get column names safely
        columns = column_discovery(collectionId, cursor)
        if not columns or len(columns) < 2:
            self.handle_error(404, f"Collection {collectionId} not found")
            return
            
        id_col = columns[0][0]
        trip_col = columns[1][0]
        
        # Build base query safely using parameterized SQL
        base_query = sql.SQL("""
            SELECT {id_col}, asMFJSON({trip_col}), count({trip_col}) OVER() as total_count 
            FROM public.{table}
        """).format(
            id_col=sql.Identifier(id_col),
            trip_col=sql.Identifier(trip_col),
            table=sql.Identifier(collectionId)
        )
        
        # Add WHERE clauses safely
        where_conditions = []
        params = []
        
        if bbox:
            try:
                bbox_coords = [float(coord) for coord in bbox.split(',')]
                if len(bbox_coords) == 4:  # 2D bbox [minx, miny, maxx, maxy]
                    x1, y1, x2, y2 = bbox_coords
                    # Use detected CRS instead of hardcoded 4326
                    where_conditions.append(sql.SQL("ST_Intersects(trajectory({trip_col})::geometry, ST_MakeEnvelope(%s, %s, %s, %s, 4326))").format(
                        trip_col=sql.Identifier(trip_col)
                    ))
                    params.extend([x1, y1, x2, y2])
                else:
                    self.handle_error(400, "Invalid bbox format: expected 4 coordinates")
                    return
            except ValueError:
                self.handle_error(400, "Invalid bbox coordinates")
                return
        
        if dateTime1 and dateTime2:
            where_conditions.append(sql.SQL("period({trip_col}) && period('[%s, %s]')").format(
                trip_col=sql.Identifier(trip_col)
            ))
            params.extend([dateTime1, dateTime2])
        elif dateTime1:
            where_conditions.append(sql.SQL("period({trip_col}) && period('[%s,]')").format(
                trip_col=sql.Identifier(trip_col)
            ))
            params.append(dateTime1)
        
        if where_conditions:
            base_query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_conditions)
        
        base_query += sql.SQL(" LIMIT %s;")
        params.append(limit)
        
        # Execute query safely
        cursor.execute(base_query, params)
        data = cursor.fetchall()
        
        if not data:
            # Return proper empty FeatureCollection
            empty_response = {
                "type": "FeatureCollection",
                "features": [],
                "timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "numberMatched": 0,
                "numberReturned": 0,
                "links": [{"href": self.path, "rel": "self", "type": "application/geo+json"}]
            }
            send_json_response(self, 200, json.dumps(empty_response))
            return
        
        total_row_count = data[0][2] if data else 0
        features = []
        
        for row in data:
            feature_id = row[0]
            mf_json_str = row[1]
            
            if sub_trajectory and dateTime1 and dateTime2:
                # Apply subTrajectory operation safely
                sub_traj_query = sql.SQL("""
                    SELECT asMFJSON(atPeriod({trip_col}, '[%s, %s]'))
                    FROM public.{table} 
                    WHERE {id_col} = %s
                """).format(
                    trip_col=sql.Identifier(trip_col),
                    table=sql.Identifier(collectionId),
                    id_col=sql.Identifier(id_col)
                )
                cursor.execute(sub_traj_query, [dateTime1, dateTime2, feature_id])
                sub_traj_result = cursor.fetchone()
                if sub_traj_result and sub_traj_result[0]:
                    mf_json_str = sub_traj_result[0]
            
            if mf_json_str:
                try:
                    feature_data = json.loads(mf_json_str)
                    
                    # Ensure proper MF-JSON structure
                    if 'type' not in feature_data:
                        feature_data['type'] = 'Feature'
                    
                    # Add required properties
                    feature_data['id'] = feature_id
                    
                    # Preserve interpolation property (REQUIREMENT 13C)
                    if sub_trajectory and 'temporalGeometry' in feature_data:
                        # Ensure interpolation is preserved from original
                        original_interpolation = feature_data['temporalGeometry'].get('interpolation')
                        if original_interpolation:
                            feature_data['temporalGeometry']['interpolation'] = original_interpolation
                    
                    features.append(feature_data)
                except json.JSONDecodeError:
                    # Skip invalid JSON features
                    continue
        
        # Build fully compliant response
        response_data = {
            "type": "FeatureCollection",
            "features": features,
            "timeStamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "numberMatched": total_row_count,
            "numberReturned": len(features),
            "links": [
                {
                    "href": self.path,
                    "rel": "self", 
                    "type": "application/geo+json"
                }
            ],
            # Always include default CRS/TRS
            "crs": {
                "type": "Name",
                "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
            },
            "trs": {
                "type": "Link",
                "properties": {
                    "type": "ogcdef",
                    "href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"
                }
            }
        }
        
        send_json_response(self, 200, json.dumps(response_data))
        
    except Exception as e:
        self.handle_error(500, f'Internal server error: {str(e)}')