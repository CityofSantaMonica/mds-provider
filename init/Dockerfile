FROM rlesouef/alpine-postgres

WORKDIR /mds

COPY . /mds

RUN chmod +x docker-entrypoint.sh

ENTRYPOINT ["/bin/bash", "docker-entrypoint.sh"]
