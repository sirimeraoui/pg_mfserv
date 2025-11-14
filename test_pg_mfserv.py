import pytest
import requests
import json
from pymeos import *
import pandas as pd
import urllib.parse
HOST = "http://localhost:8080"

pymeos_initialize()
with open("data/trajectories_mf.json", "r") as f:
    data = json.load(f)

trajectories = pd.DataFrame(data)
trajectories["trajectory"] = trajectories["trajectory"].apply(
    lambda mf: TGeogPointSeq.from_mfjson(json.dumps(mf))
)
trajectories["sog"] = trajectories["sog"].apply(
    lambda mf: TFloatSeq.from_mfjson(json.dumps(mf))
)
print("mmsi",trajectories["mmsi"][0]) #id
print("traj",trajectories["trajectory"][0])
print("sog",trajectories["sog"][0])
# print("traj",trajectories["distance"])



def log_request_response(action: str, response: requests.Response):
    req = response.request
    print(f"\n=== {action.upper()} ===")
    print(f"→ {req.method} {req.url}")
    if req.body:
        try:
            body = json.loads(req.body)
            print("Request JSON:", json.dumps(body, indent=2))
        except Exception:
            print("Request body:", req.body)
    print(f"← Status: {response.status_code}")
    try:
        print("Response JSON:", json.dumps(response.json(), indent=2))
    except Exception:
        print("Response Text:", response.text)
    print("=" * 60 + "\n")


@pytest.fixture(scope="module")
def create_collections():
    """Create initial test collections (Ships, Boats)."""
    collections = [
        {
            "title": "Ships",
            "updateFrequency": 1000,
            "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
            "itemType": "movingfeature",
        },
        {
            "title": "Boats",
            "updateFrequency": 1000,
            "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
            "itemType": "movingfeature",
        },
    ]

    created = []
    for col in collections:
        resp = requests.post(f"{HOST}/collections", json=col)
        log_request_response(f"Create collection {col['title']}", resp)
        assert resp.status_code in (200, 201, 409)  # allow existing
        created.append({
            "id": col["title"].lower(),
            "updateFrequency": col["updateFrequency"],
            "itemType": col["itemType"]
        })

    yield created  # provide IDs to tests

    # Cleanup after all tests
    for col_id in created:
        resp = requests.delete(f"{HOST}/collections/{col_id}")
        log_request_response(f"Cleanup delete {col_id}", resp)


# def test_get_all_collections(create_collections):
#     """GET /collections"""
#     resp = requests.get(f"{HOST}/collections")
#     log_request_response("Get all collections", resp)
#     assert resp.status_code == 200
#     data = resp.json()
#     collection_ids = [c["id"] for c in data.get("collections", [])]
#     for col_id in create_collections:
#         assert col_id["id"] in collection_ids


# def test_get_single_collection(create_collections):
#     """GET /collections/{id}"""
#     col_id = create_collections[0]["id"]
#     resp = requests.get(f"{HOST}/collections/{col_id}")
#     log_request_response(f"Get single collection {col_id}", resp)
#     assert resp.status_code == 200
#     data = resp.json()
#     assert data.get("id") == col_id


# def test_replace_collection(create_collections):
#     """PUT /collections/{id}"""
#     col_id = create_collections[0]["id"]

#     update_data = {
#         "title": "Vessels",
#         "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
#         "updateFrequency": 112,
#     }
#     resp = requests.put(f"{HOST}/collections/{col_id}", json=update_data)
#     log_request_response(f"Update collection {col_id}", resp)
#     assert resp.status_code in (200, 204)

#     # Verify update
#     resp = requests.get(f"{HOST}/collections/{col_id}")
#     log_request_response(f"Verify update {col_id}", resp)
#     assert resp.status_code == 200
#     data = resp.json()
#     assert data.get("title") == update_data["title"]
#     # assert data.get("description") == update_data["description"]
#     assert data.get("updateFrequency") == create_collections[0]["updateFrequency"]
#     assert data.get("itemType") in ("movingfeature" , create_collections[0]["itemType"])
    


# def test_delete_collection():
#     """DELETE /collections/{id}"""
#     tmp_data = {
#         "title": "Temp",
#         "description": "Temp collection",
#         "updateFrequency": 1,
#         "itemType": "movingfeature",
#     }

#     # Create
#     resp = requests.post(f"{HOST}/collections", json=tmp_data)
#     log_request_response("Create temp collection", resp)
#     col_id = tmp_data["title"].lower()
#     assert resp.status_code in (200, 201, 409)

#     # Delete
#     resp = requests.delete(f"{HOST}/collections/{col_id}")
#     log_request_response(f"Delete collection {col_id}", resp)
#     assert resp.status_code in (200, 204)

#     # Verify deletion
#     resp = requests.get(f"{HOST}/collections/{col_id}")
#     log_request_response(f"Verify delete {col_id}", resp)
#     assert resp.status_code == 404

# ==================== MOVING FEATURES TESTS ====================

def test_create_moving_feature(create_collections):
    """REQUIREMENT 15: POST /collections/{collectionId}/items - Create MovingFeature"""
    collection_id = "ships"
    
    # Create a test moving feature from trajectory data
    test_trajectory = trajectories.iloc[0]
    moving_feature = {
        "type": "Feature",
        "id": "test_ship_001",
        "properties": {
            "name": "Test Vessel",
            "type": "cargo",
            "mmsi": int(test_trajectory["mmsi"])
        },
        "temporalGeometry": json.loads(test_trajectory["trajectory"].as_mfjson()),
        "temporalProperties": [
            {
                "datetimes": json.loads(test_trajectory["sog"].as_mfjson())["datetimes"],
                "speed": {
                    "type": "Measure",
                    "form": "MQS",
                    "values": json.loads(test_trajectory["sog"].as_mfjson())["values"],
                    "interpolation": "Linear"
                }
            }
        ],
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
    
    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=moving_feature,
        headers={"Content-Type": "application/geo+json"}
    )
    log_request_response("Create MovingFeature", resp)
    
    # REQUIREMENT 17: Validate successful creation
    assert resp.status_code == 201
    assert "Location" in resp.headers
    assert resp.headers["Location"].endswith(f"/collections/{collection_id}/items/test_ship_001")
    
    response_data = resp.json()
    assert response_data["id"] == "test_ship_001"
    assert response_data["type"] == "Feature"
    assert "temporalGeometry" in response_data
    assert "properties" in response_data


# ____________________________________MOVING FEATURES CREATE_____________________________________________
def test_create_feature_with_provided_id(create_collections):
    """POST a single Feature with an explicit numeric ID"""
    collection_id = "ships"

    test_trajectory = trajectories.iloc[0]
    feature = {
        "type": "Feature",
        "id": int(test_trajectory["mmsi"]),  # numeric ID
        "temporalGeometry": json.loads(test_trajectory["trajectory"].as_mfjson())
    }

    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature,
        headers={"Content-Type": "application/geo+json"}
    )
    log_request_response("Create Feature with numeric ID", resp)
    assert resp.status_code in (201, 409)
    data = resp.json()
    assert str(feature["id"]) == data["id"]  # stored as TEXT
def test_create_feature_without_id(create_collections):
    """POST a single Feature with no 'id' to auto-generate"""
    collection_id = "ships"

    test_trajectory = trajectories.iloc[1]
    feature = {
        "type": "Feature",
        "temporalGeometry": json.loads(test_trajectory["trajectory"].as_mfjson())
    }

    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature,
        headers={"Content-Type": "application/geo+json"}
    )
    log_request_response("Create Feature without ID", resp)
    assert resp.status_code in (201, 409)
    data = resp.json()
    assert "id" in data
    assert len(data["id"]) > 0

def test_create_feature_collection(create_collections):
    """POST a FeatureCollection with multiple Features"""
    collection_id = "boats"

    features = []
    for idx, row in trajectories.head(3).iterrows():  # pick 3 rows for test
        features.append({
            "type": "Feature",
            "id": str(row["mmsi"]),  # string ID
            "temporalGeometry": json.loads(row["trajectory"].as_mfjson())
        })

    collection_payload = {
        "type": "FeatureCollection",
        "features": features
    }

    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=collection_payload,
        headers={"Content-Type": "application/geo+json"}
    )
    log_request_response("Create FeatureCollection", resp)
    assert resp.status_code in (201, 409)






pymeos_finalize()