import sys
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

index_name = "aoz"
model_name = "sentence-transformers__clip-vit-b-32-multilingual-v1",

es = Elasticsearch(
        "https://xxxxyyyyzzzz.es.asia-northeast1.gcp.cloud.es.io:9243",
        api_key=("REPLACE_WITH_BASE64_API_KEY"),
        request_timeout=60
        )

infer_docs = {}
infer_docs['text_field'] = sys.argv[1]

resp = es.ml.infer_trained_model(model_id=model_name, docs=infer_docs)
input_embed = resp['inference_results'][0]['predicted_value']

knn_query = {
    "field": "text_embedding.predicted_value",
    "query_vector": input_embed,
    "k": 10,
    "num_candidates": 100,
    "boost": 0.3
}

search_query = {
    "match": {
        "text_field": {
            "query": sys.argv[1],
            "boost": 0.7
        }
    }
}

search_fields = ["title", "author", "file_path"]

search_result = es.search(knn=knn_query, query=search_query, fields=search_fields, source=False)

for hit in search_result['hits']['hits']:
    print(hit['fields']['author'][0] + " - " + hit['fields']['title'][0] + " - score: " + str(hit['_score']))
