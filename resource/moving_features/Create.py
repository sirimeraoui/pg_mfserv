# REQ15: /req/movingfeatures/features-post
# REQ 17: /req/movingfeatures/features-post-success
import uuid
import json
from psycopg2 import sql
from pymeos import TGeomPoint

def post_collection_items(self, collection_id, connection, cursor):
    try:
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode("utf-8"))

        object_type = data.get("type")
        if not object_type:
            raise Exception("DataError: Missing mandatory 'type'")

        # Check the target collection exists eg ships
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            raise Exception(f'DataError: collection with id {collection_id} does not exist')

        created_feature_ids = []

        if object_type == "FeatureCollection":
            features = data.get("features")
            if not isinstance(features, list):
                raise Exception("DataError: FeatureCollection missing 'features' array")

            for feat in features:
                new_id = insert_feature(self, feat, collection_id, connection, cursor)
                if new_id:
                    created_feature_ids.append(new_id)

        elif object_type == "Feature":
            new_id = insert_feature(self, data, collection_id, connection, cursor)
            if new_id:
                created_feature_ids.append(new_id)
#object_type is neither Feature or FeatureCollection
        else:
            raise Exception("DataError: Invalid 'type'")

        connection.commit()

        # Req17: 201 POST with location headers
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        
        # Add Location header for each created feature
        for fid in created_feature_ids:
            self.send_header("Location", f"/collections/{collection_id}/items/{fid}")
        
        self.end_headers()
        
        response = {
            "message": f"Created {len(created_feature_ids)} features",
            "ids": created_feature_ids
        }
        self.wfile.write(bytes(json.dumps(response), "utf-8"))

    except Exception as e:
        msg = str(e)
        if "does not exist" in msg:
            code = 404
        elif "DataError" in msg:
            code = 400
        elif "duplicate key" in msg.lower():
            code = 409
        else:
            code = 500
        print("error", msg)
        self.handle_error(code, msg)

#add single moving feature to moving_features table
def insert_feature(self, feature, collection_id, connection, cursor):
    if feature.get("type") != "Feature":
        raise Exception("DataError: Invalid feature type")

    # generate or use given feature ID
    feat_id = feature.get("id")
    if feat_id is None:
        feat_id = str(uuid.uuid4())
    else:
        feat_id = str(feat_id)
    bbox_calculated = None
    time_range_calculated = None
    tgeom = None

    # *convert temporalGeometry to TGeomPoint
    temporal_geometry = feature.get("temporalGeometry")
    tgeom_str = None
    if temporal_geometry:#either tempGeom os given as dict or json to str
        if isinstance(temporal_geometry, dict):
            tgeom = TGeomPoint.from_mfjson(json.dumps(temporal_geometry))
            tgeom_str = str(tgeom)
        elif isinstance(temporal_geometry, str):
            tgeom = TGeomPoint.from_mfjson(temporal_geometry)
            tgeom_str = str(tgeom)
    if tgeom:
        # geometry_geojson = tgeom.as_geojson(srs="EPSG:25832")  #works 1 over 2
        # bounding box
        stbox = tgeom.bounding_box()
        # space range
        bbox_calculated = [
            stbox.xmin(), 
            stbox.ymin(), 
            stbox.xmax(),  
            stbox.ymax()  
        ]
        # time range
        time_range_calculated = [stbox.tmin().isoformat(), stbox.tmax().isoformat()]
        
    properties = feature.get("properties", {})
    geometry = feature.get("geometry")
    bbox = feature.get("bbox")
    #mf life span time range:
    time_range = feature.get("time")
    crs = feature.get("crs")
    trs = feature.get("trs")


    if bbox is None and bbox_calculated:
        bbox = bbox_calculated
        print(f"Calculated bbox for {feat_id}: {bbox}")

    if time_range is None and time_range_calculated:
        time_range = time_range_calculated
        print(f"Calculated time_range for {feat_id}: {time_range}")
    # Format time as tstzrange if provided
    time_str = None
    # time [stat, end]
    if time_range and isinstance(time_range, list) and len(time_range) == 2:
        # time_str = f"[{time_range[0]}, {time_range[1]}]" clean
        time_str = json.dumps(time_range)
#__________________________________________________________________________________check required tables exist____________________________________________
    #If moving_features table not exists, then create it
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS moving_features (
            id TEXT PRIMARY KEY,
            collection_id TEXT REFERENCES collections(id) ON DELETE CASCADE,
            type TEXT DEFAULT 'Feature',
            geometry geometry,
            properties JSONB,
            bbox JSONB,
            time_range TSTZRANGE,
            crs JSONB,
            trs JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    #If temporal_geometries table not exists, then create it
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temporal_geometries (
            id SERIAL PRIMARY KEY,
            feature_id TEXT REFERENCES moving_features(id) ON DELETE CASCADE,
            geometry_type TEXT,
            trajectory tgeompoint,
            interpolation TEXT,
            base JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    #If temporal_properties nad temporal_values tables not exists, then create
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temporal_properties (
            id SERIAL PRIMARY KEY,
            feature_id TEXT REFERENCES moving_features(id) ON DELETE CASCADE,
            property_name TEXT NOT NULL,
            property_type TEXT NOT NULL,
            form TEXT,
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS temporal_values (
            id SERIAL PRIMARY KEY,
            property_id INTEGER REFERENCES temporal_properties(id) ON DELETE CASCADE,
            datetimes TIMESTAMP[] NOT NULL,
            values JSONB NOT NULL,
            interpolation TEXT DEFAULT 'Linear',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    
    connection.commit()
#___________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________

    # INSERT INTO moving_features :temporal_geometries:Insert feature into moving_features table
    cursor.execute("""
        INSERT INTO moving_features 
        (id, collection_id, type, geometry, properties, bbox, time_range, crs, trs)
        VALUES (%s, %s, %s, trajectory(%s::tgeompoint), %s, %s, %s::tstzrange, %s, %s)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    """, (
        feat_id,
        collection_id,
        "Feature",
        tgeom_str, 
        json.dumps(properties),
        json.dumps(bbox) if bbox else None,
        time_str,
        json.dumps(crs) if crs else None,
        json.dumps(trs) if trs else None
    ))
    
    inserted = cursor.fetchone()
    
    # INSERT INTO temporal_geometries: If the create feature has a temporal_geom, then add to temporal_geometries table    
    #RE CHECK OGC (must the uiser always provide the temporal geom unsure 40 percent)
    if inserted and tgeom_str:
        geometry_type = "MovingPoint"  # Default 
        if temporal_geometry and isinstance(temporal_geometry, dict):
            geometry_type = temporal_geometry.get("type", "MovingPoint") #get geom_type of not default MovingPoint
            interpolation = temporal_geometry.get("interpolation", "Linear")
        else:
            interpolation = "Linear"
        # RE CHECK OGC: i'm assuming i receive one trajectory per inserted feature, what if it's multiple temporal_geometries (trajs) ==>
        cursor.execute("""
            INSERT INTO temporal_geometries 
            (feature_id, geometry_type, trajectory, interpolation)
            VALUES (%s, %s, %s::tgeompoint, %s)
        """, (
            feat_id,
            geometry_type,
            tgeom_str,
            interpolation
        ))
    
    if inserted:
        print(f"Inserted feature {feat_id}")
        return feat_id
    else:
        print(f"Feature {feat_id} already exists, skipped")
        return None