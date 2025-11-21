import pytest
import requests
import json
from pymeos import *
import pandas as pd
import urllib.parse
HOST = "http://localhost:8080"

pymeos_initialize()
with open("data/trajectories_mf1.json") as f:
    data = json.load(f)


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


@pytest.fixture(scope="session")
def create_collections():
    # Create initial test collections (ships, boats)
    collections = [
        {
            "title": "ships",
            "updateFrequency": 1000,
            "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
            "itemType": "movingfeature",
        },
        {
            "title": "boats",
            "updateFrequency": 1000,
            "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
            "itemType": "movingfeature",
        },
    ]

    created = []
    for col in collections:
        resp = requests.post(f"{HOST}/collections", json=col)
        log_request_response(f"Create collection {col['title']}", resp)
        assert resp.status_code in (200, 201, 409)
        created.append({
            "id": col["title"].lower(),
            "updateFrequency": col["updateFrequency"],
            "itemType": col["itemType"]
        })

    yield created

    # Cleanup
    # for col in created:
    #     col_id = col["id"]
    #     resp = requests.delete(f"{HOST}/collections/{col_id}")


# def test_get_all_collections(create_collections):
#     #gET /collections
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

# ____________________________________MOVING FEATURES CREATE_____________________________________________
def test_create_single_feature(create_collections):
    # Send first JSON object as a single Feature exactly as-is
    collection_id = "ships"
    feature_data = data[0]

    feature = {
        "type": "Feature",
        "id": str(feature_data["mmsi"]),
        "temporalGeometry": feature_data["trajectory"],
        "temporalProperties": [
            {
                "datetimes": feature_data["sog"]["datetimes"],
                "speed": feature_data["sog"]
            }
        ]
    }

    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature,
        headers={"Content-Type": "application/geo+json"}
    )
    assert resp.status_code in (201, 409)


def test_create_feature_collection():
    # Send multiple JSON objects as a FeatureCollection
    collection_id = "ships"
    features = []
    for obj in data[1:3]:
        features.append({
            "type": "Feature",
            "id": str(obj["mmsi"]),
            "temporalGeometry": obj["trajectory"],
            "temporalProperties": [
                {
                    "datetimes": obj["sog"]["datetimes"],
                    "speed": obj["sog"]
                }
            ]
        })
    feature_collection = {
        "type": "FeatureCollection",
        "features": features
    }

    resp = requests.post(
        f"{HOST}/collections/{collection_id}/items",
        json=feature_collection,
        headers={"Content-Type": "application/geo+json"}
    )
    assert resp.status_code in (201, 409)

# ____________________________________MOVING FEATURES GET_____________________________________________


def test_get_all_items(create_collections):
    # GET /collections/{id}/items returns all items
    collection_id = "ships"
    resp = requests.get(f"{HOST}/collections/{collection_id}/items")
    log_request_response(f"GET all items from {collection_id}", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    assert data_resp["type"] == "FeatureCollection"
    assert len(data_resp["features"]) > 0
    assert data_resp["numberReturned"] == len(data_resp["features"])


def test_get_items_with_limit(create_collections):
    # GET with limit parameter
    collection_id = "ships"
    limit = 1
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?limit={limit}")
    log_request_response(f"GET items with limit={limit}", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    assert len(data_resp["features"]) <= limit
    assert data_resp["numberReturned"] == len(data_resp["features"])


def test_get_items_with_bbox(create_collections):
    # GET with bbox filter
    collection_id = "ships"
    # using coordinates of the first feature
    bbox = "12.675237,54.524345,12.675237,54.524345"
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?bbox={bbox}")
    log_request_response(f"GET items with bbox={bbox}", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    # All features should intersect the bbox
    for f in data_resp["features"]:
        coords = f["temporalGeometry"]["coordinates"][0]
        x, y = coords
        x1, y1, x2, y2 = map(float, bbox.split(','))
        assert x1 <= x <= x2
        assert y1 <= y <= y2


def test_get_items_with_datetime(create_collections):
    # GET with datetime filte
    collection_id = "ships"
    # Using datetime of first feature
    dt = urllib.parse.quote("2024-03-01T00:00:00+01")
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?datetime={dt}")
    log_request_response(f"GET items with datetime={dt}", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    for f in data_resp["features"]:
        times = f["temporalGeometry"]["datetimes"]
        assert "2024-03-01T00:00:00+01" in times


def test_get_items_invalid_limit(create_collections):
   # GET with invalid limit should return 400
    collection_id = "ships"
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?limit=invalid")
    log_request_response(f"GET items with invalid limit", resp)
    assert resp.status_code == 400


def test_get_items_subtrajectory_without_datetime(create_collections):
    # subTrajectory=true without datetime should return 400
    collection_id = "ships"
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?subTrajectory=true")
    log_request_response(
        f"GET items with subTrajectory=true without datetime", resp)
    assert resp.status_code == 400


def test_get_items_subtrajectory_with_interval(create_collections):
    # subTrajectory=true with a valid datetime interval
    collection_id = "ships"
    interval = urllib.parse.quote(
        "2024-03-01T00:00:00+01/2024-03-01T01:00:00+01")
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?subTrajectory=true&datetime={interval}")
    log_request_response(
        f"GET items with subTrajectory interval={interval}", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    for f in data_resp["features"]:
        # All datetimes should be within the interval
        for dt in f["temporalGeometry"]["datetimes"]:
            assert "2024-03-01T00:00:00+01" <= dt <= "2024-03-01T01:00:00+01"


def test_get_items_leaf(create_collections):
    # GET with leaf=true should return only the last instant of each trajectory
    collection_id = "ships"
    resp = requests.get(f"{HOST}/collections/{collection_id}/items?leaf=true")
    log_request_response(f"GET items with leaf=true", resp)
    assert resp.status_code == 200
    data_resp = resp.json()
    for f in data_resp["features"]:
        coords = f["temporalGeometry"]["coordinates"]
        datetimes = f["temporalGeometry"]["datetimes"]
        # Leaf should have only one coordinate & datetime
        assert len(coords) == 1
        assert len(datetimes) == 1


def test_get_items_leaf_with_subtrajectory(create_collections):
    # GET with leaf=true and subTrajectory=true should return 400
    collection_id = "ships"
    interval = urllib.parse.quote(
        "2024-03-01T00:00:00+01/2024-03-01T01:00:00+01")
    resp = requests.get(
        f"{HOST}/collections/{collection_id}/items?leaf=true&subTrajectory=true&datetime={interval}")
    log_request_response(
        f"GET items with leaf=true & subTrajectory=true", resp)
    assert resp.status_code == 400


pymeos_finalize()
