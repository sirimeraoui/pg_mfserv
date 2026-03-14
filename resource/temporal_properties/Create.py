# REQ 37: /req/movingfeatures/tproperties-post
# REQ 39: /req/movingfeatures/tproperties-post-success
# REQ 40: /req/movingfeatures/tproperty-mandatory

from utils import send_json_response
from resource.temporal_properties.property_helper import validate_property_type, build_property_response
from resource.temporal_properties.validation_helper import (
    parse_request_body, validate_property_data, 
    validate_collection_exists, validate_feature_exists
)
import traceback
import json

# POST /collections/{collectionId}/items/{featureId}/tproperties
def post_tproperties(self, collection_id, feature_id, connection, cursor):

    try:
        data = parse_request_body(self)
        
        # Validate required fields
        errors = validate_property_data(data)
        if errors:
            self.handle_error(400, "; ".join(errors))
            return
        
        # Validate property type- REQ 40
        if not validate_property_type(data["type"]):
            self.handle_error(400, f"Invalid property type: {data['type']}. Must be one of: TBoolean, TText, TInteger, TReal, TImage")
            return
        
        # collection exists
        if not validate_collection_exists(cursor, collection_id):
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
        
        # feature exists
        if not validate_feature_exists(cursor, feature_id, collection_id):
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        
        # property already exists with same name?
        cursor.execute("""
            SELECT id FROM temporal_properties 
            WHERE feature_id = %s AND property_name = %s
        """, (feature_id, data["name"]))
        
        if cursor.fetchone() is not None:
            self.handle_error(409, f"Property '{data['name']}' already exists for this feature")
            return
        
        #otherwise, INSERT INTO temporal_properties 
        cursor.execute("""
            INSERT INTO temporal_properties 
            (feature_id, property_name, property_type, form, description)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            feature_id,
            data["name"],
            data["type"],
            data.get("form"),
            data.get("description")
        ))
        
        new_id = cursor.fetchone()[0] #clean
        connection.commit()
        
        # response REQ 37
        prop_data = {
            "property_name": data["name"],
            "property_type": data["type"],
            "form": data.get("form"),
            "description": data.get("description")
        }
        
        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        path = f"/collections/{collection_id}/items/{feature_id}/tproperties/{data['name']}"
        response = build_property_response(prop_data, base_url, path)
        
        # 201 + location  REQ 39:
        Location = f"{base_url}{path}"  #clean mod 14/03 **
        self.send_response(201)
        self.send_header("Location", Location)  #clean mod 14/03 **
        send_json_response(self, 201, response)
        
    except json.JSONDecodeError:
        self.handle_error(400, "Invalid JSON")
    except Exception as e:
        connection.rollback()
        # print(f"Error in post_tproperties: {e}")
        # traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")

        