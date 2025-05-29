from fastapi import FastAPI, Query
import weaviate
from weaviate.auth import AuthApiKey
from typing import List
from dotenv import load_dotenv
import os

# ENV variables
load_dotenv()

# ENV variables
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")

client = weaviate.connect_to_weaviate_cloud(
    cluster_url=WEAVIATE_URL,
    auth_credentials=AuthApiKey(WEAVIATE_API_KEY),
    headers={"X-OpenAI-Api-Key": OPENAI_KEY},
    skip_init_checks=True
)

app = FastAPI()

@app.get("/search_articles", response_model=List[dict])
def search_articles(q: str = Query(..., description="Search query string")):
    
    try:
        public_collection = client.collections.get("Public_Notice_Articles")
        ceqanet_collection = client.collections.get("Ceqanet_Articles")

        public_results = public_collection.query.bm25(
            query=q,
            limit=3,
            return_properties=["title", "url"]
        )

        ceqanet_results = ceqanet_collection.query.bm25(
            query=q,
            limit=3,
            return_properties=["title", "url"]
        )

        results = []
        for obj in public_results.objects:
            props = obj.properties
            results.append({
                "title": props.get("title"),
                "url": props.get("url")
            })

        for obj in ceqanet_results.objects:
            props = obj.properties
            results.append({
                "title": props.get("title"),
                "url": props.get("url")
            })

        return results

    except Exception as e:
        return [{"error": str(e)}]
