#!/bin/bash

hostName="localhost"
serverPort=8080

# OGC TEST POST COLLECTIONS:
# curl -X POST http://$hostName:$serverPort/collections \
#      -H "Content-Type: application/json" \
#      -d '{
#            "title": "Ships",
#            "updateFrequency": 1000,
#            "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
#            "itemType": "movingfeature"
#          }'
# curl -i -X POST http://$hostName:$serverPort/collections \
#      -H "Content-Type: application/json" \
#      -d '{
#            "title": "Boats",
#            "updateFrequency": 1000,
#            "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
#            "itemType": "movingfeature"
#          }'
# itemType:indicator about the type of the items in the moving features collection (the default value is 'movingfeature').

# REQUIREMENT 1
# IDENTIFIER
# /req/mf-collection/collections-get
# curl http://localhost:8080/collections
# ____________________________________________Source Collection__________________________________________________________________
#REQUIREMENT 6: OPERATION 
#IDENTIFIER /req/mf-collection/collection-get
# curl http://localhost:8080/collections/ships
# curl "http://localhost:8080/collections/ships?fields=id"

# REQUIREMENT 8 OPERATION p33
# IDENTIFIER /req/mf-collection/collection-delete
# curl -i -X DELETE http://localhost:8080/collections/ships

# REQUIREMENT 7 OPERATION
# IDENTIFIER /req/mf-collection/collection-put

curl -i -X PUT http://$hostName:$serverPort/collections/ships \
     -H "Content-Type: application/json" \
     -d '{
           "title": "Vessels",
           "description": "a collection of moving features to manage data in a distinct (physical or logical) space",
           "updateFrequency": 112
         }'


