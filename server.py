from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2, handle_error
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import pymeos_initialize, pymeos_finalize, TGeomPoint
from urllib.parse import urlparse, parse_qs
from resource.MovingFeatures import do_get_collection_items, do_post_collection_items
from resource.collections.Create import post_collections
from resource.collections.Retrieve import get_collections
from resource.collection.Retrieve import get_collection_id
from resource.collection.Delete import delete_collection
from resource.collection.Put import put_collection
pymeos_initialize()

hostName = "localhost"
serverPort = 8080

host = 'localhost'
port = 25431
db = 'postgres'
user = 'postgres'
password = 'mysecretpassword'

connection = MobilityDB.connect(host=host, port=port, database=db, user=user, password=password)
cursor = connection.cursor()


class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if 'tgsequence' in self.path:
            self.do_get_squence()
        elif 'tproperties' in self.path:
            self.do_get_tproperties()
        elif self.path == '/':
            self.do_home()
        elif self.path == '/collections':
            self.get_collections(connection,cursor)
        elif self.path.startswith('/collections') and '/items/' in self.path:
            collectionId = self.path.split('/')[2]
            feature_id = self.path.split('/')[-1]
            self.do_get_meta_data(collectionId, feature_id)
        elif '/items' in self.path and self.path.startswith('/collections/'):
            # Extract collection ID from the path
            collection_id = self.path.split('/')[2]
            self.do_get_collection_items(collection_id,connection, cursor)
        elif self.path.startswith('/collections/'):
            # Extract only the path part without query parameters
            path_only = urlparse(self.path).path
            collection_id = path_only.split('/')[-1]  # e.g., 'ships'
            self.get_collection_id(collection_id, connection, cursor)


            

    def do_get_squence(self):
        collection_id = self.path.split('/')[2]
        feature_id = self.path.split('/')[4]
        self.do_get_movement_single_moving_feature(collection_id, feature_id)

    def do_get_tproperties(self):
        collection_id = self.path.split('/')[2]
        feature_id = self.path.split('/')[4]

        if self.path.endswith("/tproperties"):
            self.do_get_set_temporal_data(collection_id,feature_id)
        else:
            tpropertyname = self.path.split('/')[6]
            self.do_get_temporal_property(collection_id, feature_id, tpropertyname)

    # POST requests router
    def do_POST(self):
        if 'tgsequence' in self.path:
            self.do_post_sequence()
        elif self.path == '/collections':
            self.post_collections(connection,cursor)
        elif '/items' in self.path and self.path.startswith('/collections/'):
            collection_id = self.path.split('/')[2]
            self.do_post_collection_items(collection_id,connection, cursor)

    def do_post_sequence(self):
        collection_id = self.path.split('/')[2]
        feature_id = self.path.split('/')[4]

        self.do_add_movement_data_in_mf(collection_id, feature_id)

    def do_DELETE(self):
        if 'tgsequence' in self.path:
            self.do_delete_sequence()
        elif self.path.startswith('/collections/') and 'items' not in self.path:
            collection_id = self.path.split('/')[-1]
            self.delete_collection(collection_id,connection, cursor)
        elif '/items' in self.path and self.path.startswith('/collections/'):
            # Extract collection ID and mFeatureId from the path
            components = self.path.split('/')
            collection_id = components[2]
            mfeature_id = components[4]
            self.do_delete_feature(collection_id, mfeature_id)

    def do_delete_sequence(self):
        components = self.path.split('/')
        collection_id = components[2]
        mfeature_id = components[4]
        tGeometry_id = self.path.split('/')[6]
        self.do_delete_single_temporal_primitive_geo(collection_id, mfeature_id, tGeometry_id)

    def do_PUT(self):
        if self.path.startswith('/collections/'):
            collection_id = self.path.split('/')[-1]
            self.do_put_collection(collection_id)

    def do_home(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(
            bytes("<html><head></head><p>Request: This is the base route of the pyApi</p>body></body></html>", "utf-8"))

    # # OGC COMPLIANT
    # # Get all collections 
    def get_collections(self,connection,cursor):
        get_collections(self,connection,cursor)
    # cleanup later
    def handle_error(self,code, message):
        handle_error(self,code, message)
    # def get_collections(self):
    #     try:
    #         cursor.execute(
    #             "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"
    #         )
    #         fetched_collections = cursor.fetchall()

    #         base_url = f"http://{hostName}:{serverPort}"

    #         collections_list = []
    #         for (table_name,) in fetched_collections:
    #             collections_list.append({
    #                 "id": table_name,
    #                 "title": table_name,
    #                 "links": [
    #                     {
    #                         "href": f"{base_url}/collections/{table_name}",
    #                         "rel": "self",
    #                         "type": "application/json"
    #                     }
    #                 ]
    #             })

    #         response = {
    #             "collections": collections_list,
    #             "links": [
    #                 {"href": f"{base_url}/collections", "rel": "self", "type": "application/json"}
    #             ]
    #         }

    #         json_data = json.dumps(response)
    #         send_json_response(self, 200, json_data)

    #     except Exception as e:
    #         self.handle_error(500, f"Internal server error: {str(e)}")

    def post_collections(self,connection,cursor):
        post_collections(self,connection,cursor)

    # def post_collections(self):
    #     try:
    #         content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
    #         post_data = self.rfile.read(content_length)  # <--- Gets the data itself
    #         print("POST request,\nPath: %s\nHeaders: %s\n\nBody: %s\n" % (
    #             self.path, self.headers, post_data.decode('utf-8')))

    #         data_dict = json.loads(post_data.decode('utf-8'))
    #         title_lower = data_dict["title"].lower().replace(" ", "_")

    #         cursor.execute(sql.SQL("DROP TABLE IF EXISTS public.{table}").format(table=sql.Identifier(title_lower)))
    #         cursor.execute(sql.SQL(
    #             "CREATE TABLE public.{table} (id SERIAL PRIMARY KEY, title TEXT, updateFrequency integer, description TEXT, itemType TEXT)").format(
    #             table=sql.Identifier(title_lower)))
    #         # cursor.execute("INSERT INTO public.moving_humans VALUES(DEFAULT, %s, %s, %s, %s)", (data_dict["title"], data_dict["updateFrequency"], data_dict["description"], data_dict["itemType"]))
    #         connection.commit()

    #         self.send_response(200)
    #         self.send_header("Content-type", "application/json")
    #         self.end_headers()
    #         self.wfile.write(bytes(post_data.decode('utf-8'), "utf-8"))
    #     except Exception as e:
    #         self.handle_error(500, 'Internal server error')
# ________________________________Resource Collection_______________________________
    def get_collection_id(self, collectionId,connection,cusor):
        get_collection_id(self, collectionId,connection,cursor)


    def delete_collection(self,collectionId,connection, cursor ):
        delete_collection(self,collectionId,connection, cursor)


    def put_collection(self, collectionId, connection, cursor):
        put_collection(self, collectionId, connection, cursor)
 #__________________________________________________________________________________________

    def do_get_collection_items(self, collectionId, connection, cursor):
        do_get_collection_items(self, collectionId,connection, cursor)
        # parsed_url = urlparse(self.path)
        # query_params = parse_qs(parsed_url.query)
        # limit = 10 if query_params.get('limit') is None else query_params.get('limit')[0]
        # x1, y1, x2, y2 = query_params.get('x1')[0], query_params.get('y1')[0], query_params.get('x2')[0], query_params.get('y2')[0]
        # subTrajectory = query_params.get('subTrajectory')[0]
        # dateTime = query_params.get('dateTime')

        # dateTime1 = dateTime[0].split(',')[0]
        # dateTime2 = dateTime[0].split(',')[1]

        # columns = column_discovery(collectionId,cursor)
        # id = columns[0][0]
        # trip = columns[1][0]

        # query = (
        #     f"SELECT {id}, asMFJSON({trip}), count(trip) OVER() as total_count "
        #     f"FROM public.{collectionId} "
        #     f"WHERE atstbox(trip, stbox 'SRID=25832;STBOX XT((({x1},{y1}), ({x2},{y2})),[{dateTime1},{dateTime2}])') IS NOT NULL "
        #     f"LIMIT {limit};"
        # )

        # cursor.execute(query)
        # row_count = cursor.rowcount
        # data = cursor.fetchall()

        # total_row_count = data[0][2]
        # crs = json.loads(data[0][1])["crs"]
        # features = []

        # for row in data:
        #     feature = json.loads(row[1])
        #     print(feature)
        #     tPoint = TGeomPoint.from_mfjson(json.dumps(feature))
        #     bbox = tPoint.bounding_box()
        #     feature["bbox"] = [bbox.xmin(), bbox.ymin(), bbox.xmax(), bbox.ymax()]
        #     feature["id"] = row[0]
        #     feature.pop("datetimes", None)
        #     features.append(feature)
        #     print(feature)
        # # Convert the GeoJSON data to a JSON string
        # geojson_data = {
        #     "type": "FeatureCollection",
        #     "features": features,
        #     "crs": crs,
        #     "timeStamp": "To be defined",
        #     "numberMatched": total_row_count,
        #     "numberReturned": row_count
        # }

        # # Convert the GeoJSON data to a JSON string
        # geojson_string = json.dumps(geojson_data)

        # # Define the coordinates of the polygon's vertices
        # send_json_response(self,200, geojson_string)

    def do_get_meta_data(self, collectionId, featureId):
        print("GET request,\nPath: %s\nHeaders: %s\n" % (self.path, self.headers))
        columns = column_discovery(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]

        try:
            sqlString = f"SELECT asMFJSON({trip}) FROM public.{collectionId} WHERE {id}={featureId};"
            cursor.execute(sqlString)

            rs = cursor.fetchall()
            if len(rs) == 0:
                raise Exception("feature does not exist")

            data = json.loads(rs[0][0])

            json_data = json.dumps(data)

            send_json_response(self,200,json_data)

        except Exception as e:
            self.handle_error(404 if "does not exist" in str(e) else 500,
                              "Collection or Feature does not exist" if "does not exist" in str(
                                  e) else str(e))

    def do_get_movement_single_moving_feature(self, collectionId, featureId):
        columns = column_discovery(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]
        try:
            parsed_url = urlparse(self.path)
            query_params = parse_qs(parsed_url.query)
            limit = 10 if query_params.get('limit') is None else query_params.get('limit')[0]
            x1, y1, x2, y2 = query_params.get('x1', [None])[0], query_params.get('y1', [None])[0], \
                query_params.get('x2', [None])[0], query_params.get('y2', [None])[0]
            if x1 or y1 or x2 or y2 is None:
                sqlString = f"SELECT {id}, {trip} FROM public.{collectionId} WHERE  {id}={featureId} LIMIT {limit};"
            else:
                dateTime = query_params.get('dateTime')
                dateTime1 = dateTime[0].split(',')[0]
                dateTime2 = dateTime[0].split(',')[-1]
                print(dateTime1, dateTime2)
                sqlString = f"SELECT {id}, asMFJSON({trip}) FROM public.{collectionId} WHERE atstbox({trip}, stbox 'SRID=25832;STBOX XT((({x1},{y1}), ({x2},{y2})),[{dateTime1},{dateTime2}])') IS NOT NULL AND {id}={featureId} LIMIT {limit};"

            subTrajectory = query_params.get('subTrajectory', [None])[0]
            leaf = query_params.get('leaf', [None])

            cursor.execute(sqlString)
            rs = cursor.fetchall()
            movements = []
            for row in rs:
                json_data = row[1].as_mfjson()  # Assuming the JSON data is in the second column of each row
                json_data = json.loads(json_data)
                movements.append(json_data)

            full_json = {
                "type": "TemporalGeometrySequence",
                "geometrySequence": movements,
                "timeStamp": "2021-09-01T12:00:00Z",
                "numberMatched": 100,
                "numberReturned": 1
            }
            json_data = json.dumps(full_json)
            send_json_response(self, 200, json_data)
            return full_json

        except Exception as e:
            print(str(e))
            self.handle_error(400, str(e))
    def do_post_collection_items(self, collectionId,connection, cursor):
        do_post_collection_items(self, collectionId,connection, cursor)
    # def do_post_collection_items(self, collectionId):
    #     try:
    #         content_length = int(self.headers['Content-Length'])
    #         post_data = self.rfile.read(content_length)
    #         print("POST request,\nPath: %s\nHeaders: %s\n\nBody: %s\n" %
    #             (self.path, self.headers, post_data.decode('utf-8')))

    #         # list of features
    #         features_list = json.loads(post_data.decode('utf-8'))
    #         if not isinstance(features_list, list):
    #             features_list = [features_list]  # wrap single feature into list

    #         for feature in features_list:
    #             feat_id = feature.get("id")
    #             tempGeo = feature.get("temporalGeometry")

    #             if tempGeo is None:
    #                 print(f"Skipping feature {feat_id}: no temporalGeometry")
    #                 continue  # skip invalid feature

    #             try:
    #                 # Validate and convert to TGeomPoint
    #                 tGeomPoint = TGeomPoint.from_mfjson(json.dumps(tempGeo))

    #                 # Insert into the collection
    #                 sql_query = f"INSERT INTO public.{collectionId} VALUES({feat_id}, '{tGeomPoint}')"
    #                 cursor.execute(sql_query)
    #                 connection.commit()
    #                 print(f"Successfully inserted feature {feat_id}")

    #             except Exception as e:
    #                 connection.rollback()
    #                 print(f"Skipping feature {feat_id} due to error: {e}")

           
    #         self.send_response(200)
    #         self.send_header("Content-type", "application/json")
    #         self.end_headers()
    #         self.wfile.write(bytes(json.dumps({"status": "done"}), "utf-8"))

    #     except Exception as e:
    #         self.handle_error(400 if "DataError" in str(e) else 500, str(e))


    def do_add_movement_data_in_mf(self, collectionId, featureId):
        columns = column_discovery(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]

        try:
            print("POST request,\nPath: %s\nHeaders: %s\n" % (self.path, self.headers))
            content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
            post_data = self.rfile.read(content_length)
            data_dict = json.loads(post_data.decode('utf-8'))

            print(data_dict)
            tgeompoint = TGeomPoint.from_mfjson(json.dumps(data_dict))



            sqlString = f"UPDATE public.{collectionId} SET {trip}= merge({trip}, '{tgeompoint}') where {id} = {featureId}"
            cursor.execute(sqlString)
            connection.commit()

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
        except Exception as e:
            self.handle_error(400, str(e))

    def do_delete_feature(self, collectionId, mfeature_id):
        columns = column_discovery(collectionId, cursor)
        id = columns[0][0]
        try:
            print("GET request,\nPath: %s\nHeaders: %s\n" % (self.path, self.headers))
            sqlString = f"DELETE FROM public.{collectionId} WHERE {id}={mfeature_id}"
            cursor.execute(sqlString)
            connection.commit()

            self.send_response(204)
            self.send_header("Content-type", "application/json")
            self.end_headers()
        except Exception as e:
            self.handle_error(404 if "does not exist" in str(e) else 500,
                              "Collection or Item does not exist" if "does not exist" in str(
                                  e) else "Server Internal Error")

    def do_delete_single_temporal_primitive_geo(self, collectionId, featureId, tGeometryId):
        columns = column_discovery(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]

        sql_select_trips = f"SELECT asMFJSON({trip}) FROM public.{collectionId} WHERE  {id}={featureId};"
        cursor.execute(sql_select_trips)
        connection.commit()
        rs = cursor.fetchall()
        print(tGeometryId)

        data_dict = json.loads(rs[0][0])
        to_change = data_dict.get("sequences")
        if to_change:
            to_change.pop(int(tGeometryId))
        else:
            to_change = data_dict.get("coordinates")
            to_change.pop(int(tGeometryId))

        print(to_change)

        if(len(to_change) == 1):
            data_dict["coordinates"] = to_change[0]
        else:
            data_dict["sequences"] = to_change

        updated_json = json.dumps(data_dict)
        tgeompoint = TGeomPoint.from_mfjson(updated_json)
        sql_update = f"UPDATE public.{collectionId} SET {trip}= '{tgeompoint}' WHERE {id}={featureId}"
        cursor.execute(sql_update)

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_get_set_temporal_data(self, collectionId, featureId):
        columns = column_discovery2(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]

        string = f"SELECT "

        for i in range(2,len(columns)):
            string+= columns[i][0] + ","

        string = string.rstrip(",")
        string += f" FROM public.{collectionId} WHERE {id} = {featureId}"
        cursor.execute(string)
        rs = cursor.fetchall()

        tab = []
        for element in rs[0]:
            mf_json = element.as_mfjson()

            tab.append(json.loads(mf_json))
        print(tab)
        json_data = {
            "temporalProperties": tab,
            "timeStamp": "2021-09-01T12:00:00Z",
            "numberMatched": 10,
            "numberReturned": 2
        }

        send_json_response(self,200,json.dumps(json_data))

        return

    def do_get_temporal_property(self,collectionId, featureId, propertyName):
        columns = column_discovery2(collectionId, cursor)
        id = columns[0][0]
        trip = columns[1][0]
        sqlString = f"SELECT asMFJSON({trip}) FROM public.{collectionId} WHERE {id} = {featureId} "
        cursor.execute(sqlString)
        rs = cursor.fetchall()
        print(rs[0][0])
        data = json.loads(rs[0][0])
        temporal_property = data.get(f"{propertyName}")

if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    connection.commit()
    cursor.close()
    pymeos_finalize()
    webServer.server_close()
    print("Server stopped.")
