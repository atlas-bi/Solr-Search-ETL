<h1 align="center">Solr Search ETL</h1>
<h4 align="center">Atlas BI Library ETL | Solr Search Engine</h4>

<p align="center">
 <a href="https://www.atlas.bi" target="_blank">Website</a> • <a href="https://demo.atlas.bi" target="_blank">Demo</a> • <a href="https://www.atlas.bi/docs/bi-library/" target="_blank">Documentation</a> • <a href="https://discord.gg/hdz2cpygQD" target="_blank">Chat</a>
</p>

<p align="center">
<a href="https://www.codacy.com/gh/atlas-bi/Solr-Search-ETL/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=atlas-bi/Solr-Search-ETL&amp;utm_campaign=Badge_Grade"><img src="https://app.codacy.com/project/badge/Grade/c2bad24b1ca447d7859f59172c48e3db"/></a>
  <a href="https://sonarcloud.io/project/overview?id=atlas-bi_Solr-Search-ETL"><img alt="maintainability" src="https://sonarcloud.io/api/project_badges/measure?project=atlas-bi_Solr-Search-ETL&metric=sqale_rating"></a>
 <a href="https://discord.gg/hdz2cpygQD"><img alt="discord chat" src="https://badgen.net/discord/online-members/hdz2cpygQD/" /></a>
 <a href="https://github.com/atlas-bi/Solr-Search-ETL/releases"><img alt="latest release" src="https://badgen.net/github/release/atlas-bi/Solr-Search-ETL" /></a>

<p align="center">Load Atlas metadata into the Solr search engine that powers Atlas' search.
 </p>

## 🏃 Getting Started

> In order to use these scripts you should already have Atlas published, with Solr started. See the [Atlas BI Libary docs](https://www.atlas.bi/docs/bi_library/).


1. Variables can either be set in the environment, or added to a `.env` file.

```py
SOLRURL=http://localhost:8983/solr/atlas
SOLRLOOKUPURL=http://localhost:8983/solr/atlas_lookups
ATLASDATABASE=DRIVER={ODBC Driver 18 for SQL Server};SERVER=server_name;DATABASE=atlas;UID=user_name;PWD=password;TrustServerCertificate=Yes;"
```

2. `delete.py` script should be run once daily to empty Solr.
3. The remaining `atlas_*.py` scripts can be run periodically through the day to keep search results current.

## 🎁 Contributing

This repository uses pre-commit and commitzen. Please commit `npm run commit && git push`.
