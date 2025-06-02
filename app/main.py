from fastapi import FastAPI, Query
import weaviate
from weaviate.auth import AuthApiKey
from typing import List
from weaviate.collections.classes.filters import Filter

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

        # filter = Filter.by_property("content").equal(q.lower())
        # public_results = public_collection.query.fetch_objects(
        #     filters=filter,
        #     return_properties=["title", "url", "content"],
        #     limit=3
        # )

        # ceqanet_results = ceqanet_collection.query.fetch_objects(
        #     filters=filter,
        #     return_properties=["title", "url", "content"],
        #     limit=3
        # )
        public_results = public_collection.query.bm25(
            query=q,
            query_properties=["content"],
            return_properties=["title", "url", "content"],
            limit=10000
        )
        ceqanet_results = ceqanet_collection.query.bm25(
            query=q,
            query_properties=["content"],
            return_properties=["title", "url", "content"],
            limit=10000
        )


        results = []
        seen_urls = set()
        for obj in public_results.objects:
            props = obj.properties
            content = props.get("content", "")
            url = props.get("url")
            match_index = content.lower().find(q.lower())
            if match_index == -1 or url in seen_urls:
                continue
            # Get a snippet of 40 characters before and after the match
            start = max(match_index - 40, 0)
            end = min(match_index + len(q) + 40, len(content))
            match_context = content[start:end]

            results.append({
                "title": props.get("title"),
                "url": url,
                # "match_position": match_index,
                "match_context": match_context
            })
            seen_urls.add(url)

        for obj in ceqanet_results.objects:
            props = obj.properties
            content = props.get("content", "")
            url = props.get("url")
            match_index = content.lower().find(q.lower())
            if match_index == -1 or url in seen_urls:
                continue
            # Get a snippet of 40 characters before and after the match
            start = max(match_index - 40, 0)
            end = min(match_index + len(q) + 40, len(content))
            match_context = content[start:end]

            results.append({
                "title": props.get("title"),
                "url": url,
                # "match_position": match_index,
                "match_context": match_context
            })
            seen_urls.add(url)
        return results

    except Exception as e:
        return [{"error": str(e)}]
