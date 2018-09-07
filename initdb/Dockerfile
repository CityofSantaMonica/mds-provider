FROM mdillon/postgis

WORKDIR /mds
COPY . /mds

ENTRYPOINT ["/bin/bash", "wait.sh", "/mds/initdb.sh"]
