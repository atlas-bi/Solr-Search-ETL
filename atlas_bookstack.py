"""Atlas ETL for Bookstack integrations."""

import os
import re
from functools import partial
from typing import Dict

import requests
from dotenv import load_dotenv

from functions import solr_load_batch

load_dotenv()

SOLRURL = os.environ.get("SOLRURL", "https://solr.example.com/solr/atlas")
BOOKSTACKURL = os.environ.get("BOOKSTACKURL", "https://docs.example.com/").strip("/")
BOOKSTACKTOKENID = os.environ.get("BOOKSTACKTOKENID", "123456")
BOOKSTACKTOKENSECRET = os.environ.get("BOOKSTACKTOKENSECRET", "78910111213")
DEFAULTVISIBILITY = os.environ.get("DEFAULTVISIBILITY", "N")

headers = {"Authorization": f"Token {BOOKSTACKTOKENID}:{BOOKSTACKTOKENSECRET}"}

download_batch_size = 50
download_offset = 0

url_params = f"?sort=-created_at&count={download_batch_size}"

pages_url = f"{BOOKSTACKURL}/api/pages?count={download_batch_size}&filter[draft]=False"

pages = requests.get(
    f"{pages_url}&offset={download_offset}", headers=headers, timeout=10, verify=False
).json()["data"]


def build_doc(page: Dict) -> Dict:
    page_data = requests.get(
        f"{BOOKSTACKURL}/api/pages/{page['id']}", headers=headers, timeout=10, verify=False
    ).json()
    page_text = requests.get(
        f"{BOOKSTACKURL}/api/pages/{page['id']}/export/plaintext",
        headers=headers,
        timeout=10,
        verify=False
    ).text

    try:
        return {
            "id": f"{BOOKSTACKURL}/books/{page['book_slug']}/page/{page_data['slug']}",
            "type": "external",
            "name": page_data["name"],
            "description": [re.sub(r"\s+", " ", page_text.replace("\n", " ")).strip()],
            "visible": DEFAULTVISIBILITY,  # "Y" if page_data["draft"] == False else "N",
            "orphan": "N",
            "runs": 10,
            # is a valid date, but we can remove ms for solr.
            "last_updated": page_data["updated_at"].split(".")[0] + "Z",
            "updated_by": str(page_data["updated_by"]["name"]),
        }
    except BaseException as e:
        print("error:")  # noqa: T201
        print(page_data)  # noqa: T201
        print(e)  # noqa: T201


while len(pages) > 0:
    batch_loader = partial(solr_load_batch, build_doc, SOLRURL)
    list(map(batch_loader, [pages]))

    download_offset += download_batch_size
    pages = requests.get(
        f"{pages_url}&offset={download_offset}", headers=headers, timeout=10
    ).json()["data"]
