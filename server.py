from http.server import BaseHTTPRequestHandler, HTTPServer

from utils import column_discovery, send_json_response, column_discovery2, handle_error
from pymeos.db.psycopg2 import MobilityDB
from psycopg2 import sql
import json
from pymeos import *
from urllib.parse import urlparse, parse_qs
from resource.collections.Create import post_collections
from resource.collections.Retrieve import get_collections
from resource.collection.Retrieve import get_collection_id
from resource.collection.Delete import delete_collection
from resource.collection.Replace import put_collection
from resource.moving_features.Create import post_collection_items, insert_feature
from resource.moving_features.Retrieve import get_collection_items
from resource.moving_feature.Retrieve import get_movement_single_moving_feature
from resource.moving_feature.Delete import delete_single_moving_feature
from resource.temporal_geom_seq.Retrieve import get_tgsequence
from resource.temporal_geom_seq.Create import post_tgsequence, add_movement_data_in_mf
from resource.temporal_prim_geom.Delete import delete_single_temporal_primitive_geo
from resource.temporal_properties.Retrieve import get_tproperties, get_set_temporal_data
from resource.temporal_properties.Create import post_tproperties
from resource.temporal_property.Retrieve import get_temporal_property
from resource.temporal_property.Delete import delete_temporal_property
from resource.temporal_property.Create import post_temporal_property
from resource.temporal_prim_value.Delete import delete_temporal_primitive_value
pymeos_initialize()

hostName = "localhost"
serverPort = 8080
host = 'localhost'
port = 25431
db = 'postgres'
user = 'postgres'
password = 'mysecretpassword'

connection = MobilityDB.connect(
    host=host, port=port, database=db, user=user, password=password)
cursor = connection.cursor()

class MyServer(BaseHTTPRequestHandler):
    # protocol_version = "HTTP/1.1"
    def do_GET(self):
        # /collections/{collectionId}/items/{mFeatureId}/tgsequence
        if 'tgsequence' in self.path:
            self.get_tgsequence(connection, cursor)
        # /collections/{collectionId}/items/{mFeatureId}/tproperties/{tPropertyName}
        elif "/tproperties/" in self.path:
            parts = self.path.split('/')
            collectionId = parts[2]
            featureId = parts[4]
            propertyName = parts[6]
            self.get_temporal_property(collectionId, featureId, propertyName, connection, cursor)
        # /collections/{collectionId}/items/{mFeatureId}/tproperties
        elif self.path.endswith("/tproperties"):
            self.get_tproperties(connection, cursor)
        # /collections/{collectionId}/items/{mFeatureId}
        elif self.path.startswith('/collections/') and '/items/' in self.path and len(self.path.split('/')) == 5:
            parts = self.path.split('/')
            collectionId = parts[2]
            mFeature_id = parts[4]
            self.get_movement_single_moving_feature(collectionId, mFeature_id, connection, cursor)
        # /collections/{collectionId}/items
        elif '/items' in self.path and self.path.startswith('/collections/'):
            collection_id = self.path.split('/')[2]
            self.get_collection_items(collection_id, connection, cursor)
        # /collections
        elif self.path == '/collections':
            self.get_collections(connection, cursor)
        # /collections/{collectionId}
        elif self.path.startswith('/collections/'):
            path_only = urlparse(self.path).path
            collection_id = path_only.split('/')[-1]
            self.get_collection_id(collection_id, connection, cursor)

        elif self.path == '/':
            self.do_home()
            # POST requests router
    def do_POST(self):
        if 'tgsequence' in self.path:
            self.post_tgsequence()
        elif self.path == '/collections':
            self.post_collections(connection, cursor)
        elif '/items' in self.path and self.path.startswith('/collections/'):
            collection_id = self.path.split('/')[2]
            self.post_collection_items(collection_id, connection, cursor)
        elif self.path.endswith("/tproperties"):
            parts = self.path.split('/')
            collectionId = parts[2]
            featureId = parts[4]
            self.post_tproperties(collectionId, featureId, connection, cursor)

        elif "/tproperties/" in self.path:
            parts = self.path.split('/')
            collectionId = parts[2]
            featureId = parts[4]
            propertyName = parts[6]

            self.post_temporal_property(collectionId, featureId, propertyName, connection, cursor)
    def do_DELETE(self):
        if 'tgsequence' in self.path:
            self.do_delete_sequence()
        elif self.path.startswith('/collections/') and 'items' not in self.path:
            collection_id = self.path.split('/')[-1]
            self.delete_collection(collection_id, connection, cursor)
            #delete single moving feature: delete /collections/{collectionId}/items/{mFeatureId}
        elif self.path.startswith('/collections/') and '/items/' in self.path and len(self.path.split('/')) == 5:
            components = self.path.split('/')
            collection_id = components[2]
            mFeature_id = components[4]
            self.delete_single_moving_feature(collection_id, mFeature_id, connection, cursor)
        elif "/tproperties/" in self.path and len(self.path.split('/')) == 8:
            #   delete /collections/{collectionId}/items/{mFeatureId}/tproperties/{tPropertyName}/{tValueId}
            parts = self.path.split('/')
            collectionId = parts[2]
            featureId = parts[4]
            propertyName = parts[6]
            tValueId = parts[7]
            self.delete_temporal_primitive_value(collectionId, featureId, propertyName, tValueId, connection, cursor)
        elif "/tproperties/" in self.path:
            # DELETE temporal property
            parts = self.path.split('/')
            collectionId = parts[2]
            featureId = parts[4]
            propertyName = parts[6]
            self.delete_temporal_property(collectionId, featureId, propertyName, connection, cursor)
    def do_PUT(self):
        if self.path.startswith('/collections/'):
            collection_id = self.path.split('/')[-1]
            self.put_collection(collection_id, connection, cursor)


    def do_home(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(
            bytes("<html><head></head><p>Request: This is the base route of the pyApi</p>body></body></html>", "utf-8"))


# ________________________________Class Moving Feature Collection_______________________________
## Resource Collections 
    def get_collections(self, connection, cursor):
        get_collections(self, connection, cursor)

    def handle_error(self, code, message):
        handle_error(self, code, message)


    def post_collections(self, connection, cursor):
        post_collections(self, connection, cursor)

## Resource Collection 

    def get_collection_id(self, collectionId, connection, cursor):
        get_collection_id(self, collectionId, connection, cursor)

    def put_collection(self, collectionId, connection, cursor):
        put_collection(self, collectionId, connection, cursor)

    def delete_collection(self, collectionId, connection, cursor):
        delete_collection(self, collectionId, connection, cursor)


 # ____________________________________________________________Class Moving features_____________________________________________________

 ## Resource Moving FeatureS

    def insert_feature(self, feature, collectionId, connection, cursor):
        insert_feature(self, feature, collectionId, connection, cursor)


    def get_collection_items(self, collectionId, connection, cursor):
        get_collection_items(self, collectionId, connection, cursor)

    def post_collection_items(self, collectionId, connection, cursor):
        post_collection_items(self, collectionId, connection, cursor)


    def do_get_meta_data(self, collectionId, featureId):
        print("GET request,\nPath: %s\nHeaders: %s\n" %
              (self.path, self.headers))
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

            send_json_response(self, 200, json_data)

        except Exception as e:
            self.handle_error(404 if "does not exist" in str(e) else 500,
                              "Collection or Feature does not exist" if "does not exist" in str(
                                  e) else str(e))
## Resource Moving Feature (single)
    #Get
    def get_movement_single_moving_feature(self, collectionId, mFeatureId, connection, cursor):
        get_movement_single_moving_feature(self, collectionId, mFeatureId, connection, cursor)

    #Delete
    def delete_single_moving_feature(self, collectionId, mFeature_id, connection, cursor):
        delete_single_moving_feature(self, collectionId, mFeature_id, connection, cursor)


## Resource Temporal Geometry Sequence
    #Get
    def get_tgsequence(self, connection, cursor):
        get_tgsequence(self, connection, cursor)
    #Post:

        #_#_____________
    def add_movement_data_in_mf(self, collectionId, featureId, connection, cursor):
        add_movement_data_in_mf(self, collectionId, featureId, connection, cursor)

    def post_tgsequence(self,connection, cursor):
        post_tgsequence(self, connection, cursor)
        #_#____________
## Resource Temporal Primitive Geomerty
    #Delete
    def delete_single_temporal_primitive_geo(self, collectionId, featureId, tGeometryId, connection, cursor):
        delete_single_temporal_primitive_geo(self, collectionId, featureId, tGeometryId, connection, cursor)

    #==========???????????
    def do_delete_sequence(self):
        components = self.path.split('/')
        collection_id = components[2]
        mfeature_id = components[4]
        tGeometry_id = self.path.split('/')[6]
        self.delete_single_temporal_primitive_geo(
            collection_id, mfeature_id, tGeometry_id)
    #==========???????????
## Resource Temporal Properties
    #Get:
        #____________________
    def get_set_temporal_data(self, collectionId, featureId,connection, cursor):
        get_set_temporal_data(self, collectionId, featureId,connection, cursor)


    def get_tproperties(self,connection, cursor):
        get_tproperties(self,connection, cursor)
        #____________________

    #Post
    def post_tproperties(self, collectionId, featureId, connection, cursor):
        post_tproperties(self, collectionId, featureId, connection, cursor)
## Resource Temporal Property
    #Get
    def get_temporal_property(self, collectionId, featureId, propertyName, connection, cursor):
        get_temporal_property(self, collectionId, featureId, propertyName, connection, cursor)
    #POst
    def post_temporal_property(self, collectionId, featureId, propertyName, connection, cursor):
        post_temporal_property(self, collectionId, featureId, propertyName, connection, cursor)
    #Delete

    def delete_temporal_property(self, collectionId, featureId, propertyName, connection, cursor):
        delete_temporal_property(self, collectionId, featureId, propertyName, connection, cursor)
## Resource Temporal Primitive Value
    def delete_temporal_primitive_value(self, collectionId, featureId, propertyName, tValueId, connection, cursor):
        delete_temporal_primitive_value(self, collectionId, featureId, propertyName, tValueId, connection, cursor)

        

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
