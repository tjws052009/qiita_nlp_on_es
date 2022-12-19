import glob
import json
import sys
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

# Variables
index_name = "aoz"
model_name = "sentence-transformers__clip-vit-b-32-multilingual-v1"
pipeline_name = "aoz-text-embedding"
repo_path = "/path/to/aozorabunko-master" # DL/clone repo from https://github.com/aozorabunko/aozorabunko

es = Elasticsearch(
        "https://xxxxyyyyzzzz.es.asia-northeast1.gcp.cloud.es.io:9243",
        api_key=("REPLACE_WITH_BASE64_API_KEY"),
        request_timeout=60
        )

es.indices.delete(index=index_name, ignore_unavailable=True)

pipeline_description = "text embedding for Aozora Bunko"
pipeline_processors = [
    {
        "inference": {
            "model_id": model_name,
            "target_field": "text_embedding",
            "ignore_failure": False,
        }
    }
]

es.ingest.put_pipeline(id=pipeline_name, description=pipeline_description, processors=pipeline_processors)

aoz_mapping = {
    "properties": {
        "title": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "author": {
            "type": "keyword"
        },
        "text_field": {
            "type": "text"
        },
        "publish_year": {
            "type": "integer"
        },
        "text_embedding.predicted_value": {
          "type": "dense_vector",
          "dims": 512,
          "index": True,
          "similarity": "cosine"
        },
        "file_path": {
            "type": "keyword"
        }
    }
}

aoz_settings = {}
aoz_settings["index.default_pipeline"] = pipeline_name

es.indices.create(index=index_name, mappings=aoz_mapping, settings=aoz_settings)

path = repo_path + "/cards/**/files/*_*.html"

aoz_bulk = []
for ind, fn in enumerate(glob.glob(path, recursive=True)): 
    print(fn)
    f = open(fn, "r", encoding="shift-jis", errors='ignore')

    soup = BeautifulSoup(f.read(), 'html.parser')

    try: 
        title = soup.find("h1", class_="title").text
        author = soup.find("h2", class_="author").text
        main_text = soup.find("div", class_="main_text").text
        publish_year = str(soup.find("div", class_="bibliographical_information")).split("<br/>")[2].strip()[0:4]

        aoz = {}
        aoz['title'] = title
        aoz['author'] = author
        aoz['text_field'] = main_text
        aoz['publish_year'] = publish_year
        aoz['file_path'] = "/" + "/".join(fn.split('/')[-4:])

        aoz_data = json.dumps(aoz, ensure_ascii=False)
        # print(aoz_data)
        aoz_bulk.append({"index": {"_index": index_name}})
        aoz_bulk.append(aoz_data)
    except:
        print("skip due to error")
        
    if ind % 100 == 0 and len(aoz_bulk) > 0:
        print("bulk at " + str(ind))
        # print(aoz_bulk)
        resp = es.bulk(index=index_name, body=aoz_bulk)
        print(resp)
        aoz_bulk = []

        if ind > 1000:
            sys.exit(0)
