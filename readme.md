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

### Dependencies

#### Java

For development purposes, sorl search can be started directly from the Atlas source code.

1. Install [Java JRE](https://www.oracle.com/java/technologies/downloads/)
2. Add a system environment variable called `JAVA_HOME` with the path to java, for example `C:\Program Files\Java\jdk-17.0.1`.
3. In your terminal navigate to `/web/solr/` in the Atlas source code. Run `./bin/solr start` to start solr.

#### Python

This ETL uses python > 3.7. Python can be installed from [https://www.python.org/downloads/](https://www.python.org/downloads/)

[C++ build tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) are needed on Windows OS.

[ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) is required for connecting to the database.

### Install Packages

This ETL uses `poetry` as the package manager. Alternatively, you can use `pip` to install the dependencies listed in `pyproject.toml`/dependencies.

```bash
poetry install
```

### Create a `.env` file

Variables can either be set in the environment, or added to a `.env` file.

```env
SOLRURL=http://localhost:8983/solr/atlas
SOLRLOOKUPURL=http://localhost:8983/solr/atlas_lookups
ATLASDATABASE=DRIVER={ODBC Driver 18 for SQL Server};SERVER=server_name;DATABASE=atlas;UID=user_name;PWD=password;TrustServerCertificate=Yes;"

# Optional for bookstack etl
BOOKSTACKURL=https://docs.example.com
BOOKSTACKTOKENID=123456
BOOKSTACKTOKENSECRET=78910111213
```

### Running

`delete.py` script should be run once daily to empty Solr.

The remaining `atlas_*.py` scripts can be run periodically through the day to keep search results current.

```bash
poetry run python delete.py
poetry run python atlas_collections.py
poetry run python atlas_initiatives.py
poetry run python atlas_groups.py
poetry run python atlas_terms.py
poetry run python atlas_lookups.py
poetry run python atlas_users.py
poetry run python atlas_reports.py

# Optional etl to load documents from bookstack. Use this as an example etl for loading external content into search!
poetry run python atlas_bookstack.py
```

## 🎁 Contributing

This repository uses pre-commit and commitzen. Please commit `npm run commit && git push`.
