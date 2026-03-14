# REQ 41: /req/movingfeatures/tproperty-get
# REQU 44: /req/movingfeatures/tproperty-get-success

from utils import send_json_response
from resource.temporal_properties.property_helper import build_property_values_response
import json
import traceback


# GET /collections/{collectionId}/items/{featureId}/tproperties/{propertyName}
def get_temporal_property(self, collection_id, feature_id, property_name, connection, cursor):

    try:
       # collection && feature exist:+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        cursor.execute(
            "SELECT id FROM collections WHERE id = %s",
            (collection_id,)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Collection '{collection_id}' not found")
            return
    
        cursor.execute(
            "SELECT id FROM moving_features WHERE id = %s AND collection_id = %s",
            (feature_id, collection_id)
        )
        if cursor.fetchone() is None:
            self.handle_error(404, f"Feature '{feature_id}' not found in collection '{collection_id}'")
            return
        
        # Get pproperty metadata
        cursor.execute("""
            SELECT id, property_name, property_type, form, description
            FROM temporal_properties
            WHERE feature_id = %s AND property_name = %s
        """, (feature_id, property_name))
        
        prop_row = cursor.fetchone()
        if prop_row is None:
            self.handle_error(404, f"Property '{property_name}' not found for feature '{feature_id}'")
            return
        
        property_id = prop_row[0] #get property values
        property_data = {
            "property_name": prop_row[1],
            "property_type": prop_row[2],
            "form": prop_row[3],
            "description": prop_row[4]
        }
        
        # Get values WHERE property_id = %s
        cursor.execute("""
            SELECT id, datetimes, values, interpolation
            FROM temporal_values
            WHERE property_id = %s
            ORDER BY datetimes[1]
        """, (property_id,))
        
        value_rows = cursor.fetchall()
        
        values = []
        for row in value_rows:
            values.append({
                "id": row[0],
                "datetimes": [dt.isoformat() for dt in row[1]],
                "values": row[2],
                "interpolation": row[3]
            })
        
        # response
        base_url = f"http://{self.server.server_name}:{self.server.server_port}"
        path = f"/collections/{collection_id}/items/{feature_id}/tproperties/{property_name}"
        
        response = build_property_values_response(property_data, values, base_url, path)
        
        send_json_response(self, 200, response)
        
    except Exception as e:
        connection.rollback()
        # print(f"Error in get_temporal_property: {e}")
        # traceback.print_exc()
        self.handle_error(500, f"Internal server error: {str(e)}")