from utils import send_json_response
import json

def build_collection_response(collection, base_url):
    return {
        "id": collection['id'],
        "title": collection['title'],
        "description": collection['description'],
        "itemType": collection['item_type'],
        "updateFrequency": collection['update_frequency'],
        "extent": None,  
        "links": [
            {
                "href": f"{base_url}/collections/{collection['id']}",
                "rel": "self",
                "type": "application/json"
            },
            {
                "href": f"{base_url}/collections/{collection['id']}/items",
                "rel": "items",
                "type": "application/json"
            }
        ]
    }


def build_collections_list_response(collections, base_url):
    return {
        "collections": collections,
        "links": [
            {
                "href": f"{base_url}/collections",
                "rel": "self",
                "type": "application/json"
            }
        ]
    }


def fetch_collection_by_id(cursor, collection_id):
    cursor.execute("""
        SELECT id, title, description, update_frequency, item_type
        FROM collections
        WHERE id = %s
    """, (collection_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
    
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def fetch_all_collections(cursor):
    cursor.execute("""
        SELECT id, title, description, update_frequency, item_type
        FROM collections
        ORDER BY created_at DESC
    """)
    
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    #CONSTRUCT THE collections dict
    collections = []
    for row in rows:
        collections.append(dict(zip(columns, row)))
    
    return collections




####################################################"Create and Replace helpers"
def validate_collection_data(data, is_update=False):
    errors = []
    validated = {}
    #if create aka post: operation
    if not is_update:
        # Post: Title mandatory:
        if "title" not in data:
            errors.append("Missing required field: title")
        else:
            validated["title"] = data["title"] 
            validated["id"] = data["title"].lower().replace(" ", "_")
    else: # Replace aka Put operation:
        if "title" in data:
            validated["title"] = data["title"]
    
    # Optional fields for POST AND PUT COLLECTION
    if "description" in data:
        validated["description"] = data["description"]
    
    if "itemType" in data:#PUT
        if data["itemType"] != "movingfeature":
            errors.append("itemType must be 'movingfeature'")
        validated["itemType"] = data["itemType"]
    elif not is_update:#POST
        validated["itemType"] = "movingfeature"
    
    if "updateFrequency" in data and not is_update:
        # updateFrequency can be set only for POST,  can't be replaced:
        validated["updateFrequency"] = data["updateFrequency"]
    

    return errors, validated

# Check collection existance by ID:
def collection_exists(cursor, collection_id):
    cursor.execute(
        "SELECT id FROM collections WHERE id = %s",
        (collection_id,)
    )
    return cursor.fetchone() is not None


#________________________________rr
def insert_collection(cursor, collection_id, data):
    cursor.execute("""
        INSERT INTO collections 
        (id, title, description, update_frequency, item_type)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (
        collection_id,
        data.get("title"),
        data.get("description"),
        data.get("updateFrequency"),
        data.get("itemType", "movingfeature")
    ))
    return cursor.fetchone()[0]

def update_collection(cursor, collection_id, data):
    updates = []
    values = []
    
    if "title" in data:
        updates.append("title = %s")
        values.append(data["title"])
    if "description" in data:
        updates.append("description = %s")
        values.append(data["description"])
    if "itemType" in data:
        updates.append("item_type = %s")
        values.append(data["itemType"])
    updates.append("updated_at = NOW()")
    
    if updates:
        values.append(collection_id)
        cursor.execute(f"""
            UPDATE collections 
            SET {', '.join(updates)} 
            WHERE id = %s
        """, values)
        return True
    return False