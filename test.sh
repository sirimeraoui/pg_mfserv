#!/bin/bash

hostName="localhost"
serverPort=8080

# OGC TEST POST COLLECTIONS:
curl -X POST http://$hostName:$serverPort/collections \
     -H "Content-Type: application/json" \
     -d '{
           "title": "Ships",
           "updateFrequency": 1000,
           "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
           "itemType": "movingfeature"
         }'
curl -i -X POST http://$hostName:$serverPort/collections \
     -H "Content-Type: application/json" \
     -d '{
           "title": "Boats",
           "updateFrequency": 1000,
           "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
           "itemType": "movingfeature"
         }'
# itemType:indicator about the type of the items in the moving features collection (the default value is 'movingfeature').

# REQUIREMENT 1
# IDENTIFIER
# /req/mf-collection/collections-get
# curl http://localhost:8080/collectionss