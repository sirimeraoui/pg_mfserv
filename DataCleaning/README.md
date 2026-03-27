<!-- Based on section 9.2 AIS Data Cleaning from the MobilityDataScience book
#and https://github.com/mahmsakr/MobilityDataScienceClass/tree/main/Mobility%20Data%20Cleaning-->
wsl -l -v
wsl -d Ubuntu-22.04
sudo service docker start (sudo apt install docker.io -y)
<!-- For DEMO -->
- docker pull --platform=linux/amd64 mobilitydb/mobilitydb  (permission issue? do sudo groupadd docker and sudo usermod -aG docker $USER and newgrp docker)
- docker volume create mobilitydb_data
- docker run --name mobilitydb -e POSTGRES_PASSWORD=mysecretpassword -p 25431:5432 -v mobilitydb_data:/var/lib/postgresql -d mobilitydb/mobilitydb
- alternative (docker start mobilitydb)
- python -m venv name
- pip install -r req.txt if there is one