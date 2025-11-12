import pytest
import requests
import json

HOST = "http://localhost:8080"



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


def test_get_all_collections(create_collections):
    """GET /collections"""
    resp = requests.get(f"{HOST}/collections")
    log_request_response("Get all collections", resp)
    assert resp.status_code == 200
    data = resp.json()
    collection_ids = [c["id"] for c in data.get("collections", [])]
    for col_id in create_collections:
        assert col_id["id"] in collection_ids


def test_get_single_collection(create_collections):
    """GET /collections/{id}"""
    col_id = create_collections[0]["id"]
    resp = requests.get(f"{HOST}/collections/{col_id}")
    log_request_response(f"Get single collection {col_id}", resp)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("id") == col_id


def test_replace_collection(create_collections):
    """PUT /collections/{id}"""
    col_id = create_collections[0]["id"]

    update_data = {
        "title": "Vessels",
        "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
        "updateFrequency": 112,
    }
    resp = requests.put(f"{HOST}/collections/{col_id}", json=update_data)
    log_request_response(f"Update collection {col_id}", resp)
    assert resp.status_code in (200, 204)

    # Verify update
    resp = requests.get(f"{HOST}/collections/{col_id}")
    log_request_response(f"Verify update {col_id}", resp)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("title") == update_data["title"]
    # assert data.get("description") == update_data["description"]
    assert data.get("updateFrequency") == create_collections[0]["updateFrequency"]
    assert data.get("itemType") in ("movingfeature" , create_collections[0]["itemType"])
    


def test_delete_collection():
    """DELETE /collections/{id}"""
    tmp_data = {
        "title": "Temp",
        "description": "Temp collection",
        "updateFrequency": 1,
        "itemType": "movingfeature",
    }

    # Create
    resp = requests.post(f"{HOST}/collections", json=tmp_data)
    log_request_response("Create temp collection", resp)
    col_id = tmp_data["title"].lower()
    assert resp.status_code in (200, 201, 409)

    # Delete
    resp = requests.delete(f"{HOST}/collections/{col_id}")
    log_request_response(f"Delete collection {col_id}", resp)
    assert resp.status_code in (200, 204)

    # Verify deletion
    resp = requests.get(f"{HOST}/collections/{col_id}")
    log_request_response(f"Verify delete {col_id}", resp)
    assert resp.status_code == 404