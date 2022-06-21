"""Delete script is run once daily to clean out any removed data."""
import os

import pysolr
from dotenv import load_dotenv

load_dotenv()

SOLRURL = os.environ.get("SOLRURL", "https://solr.example.com/solr/atlas")
SOLRLOOKUPURL = os.environ.get(
    "SOLRLOOKUPURL", "https://solr.example.com/solr/atlas_lookups"
)

solr = pysolr.Solr(SOLRURL, always_commit=True)
solr.delete(q="*:*")
solr.optimize()


solr = pysolr.Solr(SOLRLOOKUPURL, always_commit=True)
solr.delete(q="*:*")
solr.optimize()
