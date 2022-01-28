"""Delete script is run once daily to clean out any removed data."""

import pysolr
import settings

solr = pysolr.Solr(settings.SOLR_URL, always_commit=True)
solr.delete(q="*:*")
solr.optimize()


solr = pysolr.Solr(settings.SOLR_LOOKUP_URL, always_commit=True)
solr.delete(q="*:*")
solr.optimize()
