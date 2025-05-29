import requests
import weaviate
from weaviate.auth import AuthApiKey
import json
from weaviate.collections.classes.config import Property, DataType
from dotenv import load_dotenv
import os

load_dotenv()

# ENV variables
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")
public_notice_APIFY_DATASET_ID = os.getenv("PUBLIC_NOTICE_APIFY_DATASET_ID")
ceqanet_APIFY_DATASET_ID = os.getenv("CEQANET_APIFY_DATASET_ID")

client = weaviate.connect_to_weaviate_cloud(
    cluster_url=WEAVIATE_URL,
    auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
    headers={"X-OpenAI-Api-Key": OPENAI_KEY},
    skip_init_checks=True
)


if not client.collections.exists("Public_Notice_Articles"):
    client.collections.create(
    "Public_Notice_Articles",
    properties = [
        Property(name="title", data_type=DataType.TEXT),
        Property(name="type", data_type=DataType.TEXT),
        Property(name="url", data_type=DataType.TEXT),
        Property(name="content", data_type=DataType.TEXT)
    ],
    )
if not client.collections.exists("Ceqanet_Articles"):
    client.collections.create(
    "Ceqanet_Articles",
    properties = [
        Property(name="source", data_type=DataType.TEXT),
        Property(name="SCHNumber", data_type=DataType.TEXT),
        Property(name="Type", data_type=DataType.TEXT),
        Property(name="Date", data_type=DataType.TEXT),
        Property(name="title", data_type=DataType.TEXT),
        Property(name="LeadPublicAgency", data_type=DataType.TEXT),
        Property(name="url", data_type=DataType.TEXT),
        Property(name="content", data_type=DataType.TEXT)
    ],
    )





# Get public-notice scraped data from Apify
public_notice_apify_url = f"https://api.apify.com/v2/datasets/{public_notice_APIFY_DATASET_ID}/items?clean=true"
public_notice_res = requests.get(public_notice_apify_url)
public_notice_res.raise_for_status()
public_notice_docs = public_notice_res.json()


# Get ceqanet scraped data from Apify
ceqanet_apify_url = f"https://api.apify.com/v2/datasets/{ceqanet_APIFY_DATASET_ID}/items?clean=true"
ceqanet_res = requests.get(ceqanet_apify_url)
ceqanet_res.raise_for_status()
ceqanet_docs = ceqanet_res.json()

# Ingest documents
Public_Notice_collection = client.collections.get("Public_Notice_Articles")
for doc in public_notice_docs:
    Public_Notice_collection.data.insert(
        properties={
        "title": doc.get("title", ""),
        "type": doc.get("type", ""),
        "url": doc.get("url", ""),
        "content": doc.get("content", ""),
    }
    )

Ceqanet_collection = client.collections.get("Ceqanet_Articles")

for doc in ceqanet_docs:
    content_raw = doc.get("content", "")
    if not isinstance(content_raw, str):
        content_str = json.dumps(content_raw)
    else:
        content_str = str(content_raw)

    Ceqanet_collection.data.insert(
        properties={
        "source": doc.get("source", ""),
        "SCHNumber": doc.get("SCHNumber", ""),
        "Type": doc.get("Type", ""),
        "Date": doc.get("Date", ""),
        "title": doc.get("title", ""),
        "LeadPublicAgency": doc.get("Lead/Public_Agency", ""),
        "url": doc.get("url", ""),
        "content": content_str,

    }
    )

print("✅ Ingestion complete.")
client.close()

