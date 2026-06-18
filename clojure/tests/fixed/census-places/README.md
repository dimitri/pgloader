# census-places test

Exercises `LOAD ARCHIVE` + `LOAD FIXED` in combination: pgloader downloads a
ZIP archive over HTTP, finds the matching file inside it by name pattern, and
loads it as a fixed-width text file.

## Source data

**Original URL**:
```
https://www2.census.gov/geo/docs/maps-data/data/gazetteer/places2k.zip
```
*(US Census Bureau — Census 2000 Gazetteer of Places, ~25 000 incorporated
places and Census Designated Places across all US states and territories)*

The archive contains one file, `places2k.txt`, encoded in latin1 with
fixed-width columns (164 bytes per record).  Only four of the twelve columns
are loaded into the target table (`usps`, `fips`, `fips_code`,
`"LocationName"`); the remaining columns (population, housing units, land/water
area, lat/lon) are parsed but discarded via the column projection.

## Why it is mirrored locally

The census.gov server uses Cloudflare and returns HTTP/2 streams that some
versions of `curl` do not handle cleanly (stream closes with
`INTERNAL_ERROR` before the transfer completes).  To avoid flaky CI, the file
is mirrored at `clojure/tests/fixtures/http/places2k.zip` (~1 MB) and served
at `http://fileserver/places2k.zip` by the `fileserver` Docker Compose service
(`joseluisq/static-web-server:2`) defined in
`tests/file-based/docker-compose.yml`.

## Refreshing the mirror

If the fixture needs to be updated, use Python's `urllib` which handles the
Cloudflare redirect without the HTTP/2 issue:

```sh
python3 -c "
import urllib.request
url = 'https://www2.census.gov/geo/docs/maps-data/data/gazetteer/places2k.zip'
req = urllib.request.Request(url, headers={'User-Agent': 'pgloader-test'})
with urllib.request.urlopen(req) as r, open('clojure/tests/fixtures/http/places2k.zip', 'wb') as f:
    f.write(r.read())
print('done')
"
```

Verify the download before committing:

```sh
unzip -t clojure/tests/fixtures/http/places2k.zip
```

Commit the updated file.
