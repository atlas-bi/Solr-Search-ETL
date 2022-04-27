# Atlas BI Library Search ETL

Atlas BI Library > 2021.11.1 uses Apache Solr as the search engine. This collection of script is used to load data into Solr.

In order to use these scripts you should already have Atlas published, with Solr started. See the [Atlas BI Libary docs](https://www.atlas.bi/docs/bi_library/).

## How to use

1. Create a `settings.py` file with two variables, update as needed:

```py
SOLR_URL = "http://localhost:8983/solr/atlas"
SOLR_LOOKUP_URL = "http://localhost:8983/solr/atlas_lookups"
SQL_CONN = "SERVER=server_name;DATABASE=atlas;UID=user_name;PWD=password"
```

2. `delete.py` script should be run once daily to empty Solr.
3. The remaining `atlas_*.py` scripts can be run periodically through the day to keep search results current.

## Contributing

This repository uses pre-commit and commitzen. Please commit `npm run commit && git push`.
