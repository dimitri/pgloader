# MariaDB with employees sample database pre-seeded at build time.
# The employees tarball is fetched from GitHub during docker build so
# CI caches the image layer and avoids re-downloading on every run.
FROM mariadb:11

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Download and extract employees test database (35 MB tarball → 168 MB data)
RUN mkdir /employees-data \
 && curl -fsSL \
      "https://codeload.github.com/datacharmer/test_db/tar.gz/refs/tags/v1.0.7" \
    | tar xz --strip-components=1 -C /employees-data/

COPY init-employees.sh /docker-entrypoint-initdb.d/init-employees.sh
RUN chmod +x /docker-entrypoint-initdb.d/init-employees.sh
