import json 
from datetime import datetime

import re
from datetime import datetime
def build_feature_from_row(row, collection_id, include_temporal=True):
    geometry_json = row[2]
    if geometry_json and isinstance(geometry_json, str):
        try:
            geometry = json.loads(geometry_json)
        except:
            geometry = None
    else:
        geometry = None
    feature = {
        "type": "Feature",
        "id": str(row[0]),
        "geometry": geometry,
        "properties": row[3] or {},
        "bbox": row[4],
        "crs": row[6],
        "trs": row[7],
        "links": [
            {
                "href": f"/collections/{collection_id}/items/{row[0]}",
                "rel": "self",
                "type": "application/json"
            },
            {
                "href": f"/collections/{collection_id}/items/{row[0]}/tgsequence",
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/temporal-geometry",
                "type": "application/json"
            },
            {
                "href": f"/collections/{collection_id}/items/{row[0]}/tproperties",
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/temporal-properties",
                "type": "application/json"
            }
        ]
    }
    
    #Parse time_range if exists (col indx 5)
    if row[5]:
        time_str = row[5]
        if time_str and time_str.startswith('[') and time_str.endswith(']'):
            times = time_str[1:-1].split(',')
            feature["time"] = [t.strip() for t in times]
    
    # Parse temporal geometries if included col 8 ***************Only for Retrieve single moving feature
    if include_temporal and len(row) > 8 and row[8]:
        temporal_geometries = []
        tg_list = row[8]
        for tg in tg_list:
            if tg.get('trajectory'):
                # trajectory bjson to dict
                traj = json.loads(tg['trajectory'])
                temporal_geometries.append({
                    "id": tg['id'],
                    "type": tg['type'],
                    "datetimes": traj.get('datetimes', []),
                    "coordinates": traj.get('coordinates', []),
                    "interpolation": tg['interpolation'],
                    "base": tg['base']
                })
        feature["temporalGeometry"] = temporal_geometries
    # *********************************************************************
    return feature

#with pagination next links (class dgrm ogc)
def build_feature_collection_response(features, total_count, limit, base_url, path, 
                                      bbox=None, datetime_param=None):
#deprecated utcnow clean 
    response = {
        "type": "FeatureCollection",
        "features": features,
        "timeStamp": datetime.utcnow().isoformat() + "Z",
        "numberMatched": total_count,
        "numberReturned": len(features),
        "links": [
            {
                "href": f"{base_url}{path}",
                "rel": "self",
                "type": "application/json"
            }
        ]
    }

    # Next page >>
    if total_count > limit:
        next_params = f"limit={limit}&offset={limit}"
        if bbox:
            next_params += f"&bbox={bbox}"
        if datetime_param:
            next_params += f"&datetime={datetime_param}"
        #next link:
        response["links"].append({
            "href": f"{base_url}{path}?{next_params}",
            "rel": "next",
            "type": "application/json"
        })

    return response





