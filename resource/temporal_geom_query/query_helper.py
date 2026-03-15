from datetime import datetime
#req 33 , response obj for temporal geometries distance, velocity and acceleration queries
def build_query_response(values, unit, query_type, base_url, path):
    return {
        "type": "TReal",
        "values": values,  # [{time, value}{}]
        "unit": unit,
        "queryType": query_type,
        "links": [
            {
                "href": f"{base_url}{path}",
                "rel": "self",
                "type": "application/json"
            }
        ],
        "timeStamp": datetime.utcnow().isoformat() + "Z"
    }