# DBF test suite

Tests loading dBASE `.dbf` files into PostgreSQL.

## Tests

| Test | Source file | Notes |
|---|---|---|
| `dbf-31` | `dbase_31.dbf` | dBASE III format |
| `dbf-8b` | `dbase_8b.dbf` | dBASE 8-byte numeric fields |
| `reg2013` | `reg2013.dbf` | INSEE French regions 2013, cp850 encoding, integer casts |
| `dbf-memo` | `DNORDOC.DBF` | File with memo fields (`.DBT` sidecar), cp866 encoding |
| `dbf-zip` | `france2016-dbf.zip` | ZIP-wrapped DBF fetched over HTTP (see below) |

All source fixtures live under `../fixtures/dbf/` (i.e.
`clojure/tests/fixtures/dbf/` from the repo root).

## HTTP-sourced test: dbf-zip

`dbf-zip` exercises pgloader's ability to fetch and unpack a ZIP archive
from an HTTP URL, then load the `.dbf` file inside it.

**Original URL**:
```
https://www.insee.fr/fr/statistiques/fichier/2114819/france2016-dbf.zip
```
*(INSEE — French National Institute of Statistics and Economic Studies,
France 2016 communes, dBASE format, cp850 encoding, ~36 000 rows)*

To avoid network dependencies in CI the file is mirrored locally at
`clojure/tests/fixtures/http/france2016-dbf.zip` and served at
`http://fileserver/france2016-dbf.zip` by the `fileserver` service in
`tests/file-based/docker-compose.yml` (`joseluisq/static-web-server:2`).

### Refreshing the mirror

If the fixture needs to be updated:

```sh
python3 -c "
import urllib.request
url = 'https://www.insee.fr/fr/statistiques/fichier/2114819/france2016-dbf.zip'
req = urllib.request.Request(url, headers={'User-Agent': 'pgloader-test'})
with urllib.request.urlopen(req) as r, open('clojure/tests/fixtures/http/france2016-dbf.zip', 'wb') as f:
    f.write(r.read())
"
```

Commit the updated file.
