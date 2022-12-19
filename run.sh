ESS_API_KEY=REPLACE_WITH_BASE64_API_KEY
ESS_URL=https://xxxxyyyyzzzz.es.asia-northeast1.gcp.cloud.es.io:9243

HUB_MODEL_ID="sentence-transformers/clip-ViT-B-32-multilingual-v1"
TASK_TYPE="text_embedding"

eland_import_hub_model --es-api-key --url $ESS_URL --hub-model-id $HUB_MODEL_ID --task-type $TASK_TYPE --start
