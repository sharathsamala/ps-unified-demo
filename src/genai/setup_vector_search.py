"""Provision a Vector Search endpoint + Delta-sync index for the parts catalog.

- Endpoint: STANDARD, idempotent — reused if it already exists.
- Index: DELTA_SYNC over `<catalog>.parts.parts_searchable`, managed embeddings
  via `databricks-gte-large-en`, primary key `part_id`, embedding source
  column `description`. Pipeline type TRIGGERED — kicked once at create time
  and on each rerun.
- Idempotent: if the index exists, triggers a sync instead of recreating.

Vector Search resources do not have first-class DAB support yet, so we drive
them through the SDK. Embedding model uses the workspace's built-in
foundation-model serving — no extra setup needed.
"""

import argparse
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)


EMBEDDING_MODEL = "databricks-gte-large-en"
PRIMARY_KEY = "part_id"
EMBEDDING_SOURCE_COLUMN = "description"
ENDPOINT_READY_TIMEOUT_S = 30 * 60
INDEX_READY_TIMEOUT_S = 20 * 60


def ensure_endpoint(w, name):
    try:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=name)
        state = ep.endpoint_status.state if ep.endpoint_status else None
        print(f"Endpoint '{name}' exists (state={state})")
    except Exception:
        print(f"Creating endpoint '{name}' (STANDARD) — first provision takes ~5 min")
        w.vector_search_endpoints.create_endpoint(
            name=name,
            endpoint_type=EndpointType.STANDARD,
        )

    deadline = time.time() + ENDPOINT_READY_TIMEOUT_S
    while time.time() < deadline:
        ep = w.vector_search_endpoints.get_endpoint(endpoint_name=name)
        state = ep.endpoint_status.state if ep.endpoint_status else None
        if str(state).endswith("ONLINE"):
            print(f"Endpoint '{name}' ONLINE")
            return
        print(f"  endpoint state={state} — waiting…")
        time.sleep(20)
    raise TimeoutError(f"Endpoint '{name}' did not reach ONLINE within timeout")


def ensure_index(w, endpoint_name, index_name, source_table):
    try:
        idx = w.vector_search_indexes.get_index(index_name=index_name)
        print(f"Index '{index_name}' exists — triggering sync")
        try:
            w.vector_search_indexes.sync_index(index_name=index_name)
        except Exception as e:
            print(f"[warn] sync trigger failed (will rely on auto-sync): {e}")
    except Exception:
        print(f"Creating Delta-sync index '{index_name}' over {source_table}")
        w.vector_search_indexes.create_index(
            name=index_name,
            endpoint_name=endpoint_name,
            primary_key=PRIMARY_KEY,
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=source_table,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name=EMBEDDING_SOURCE_COLUMN,
                        embedding_model_endpoint_name=EMBEDDING_MODEL,
                    ),
                ],
            ),
        )

    deadline = time.time() + INDEX_READY_TIMEOUT_S
    while time.time() < deadline:
        idx = w.vector_search_indexes.get_index(index_name=index_name)
        status = idx.status
        ready = getattr(status, "ready", False) if status else False
        msg = getattr(status, "message", "") if status else ""
        if ready:
            print(f"Index '{index_name}' READY — {msg}")
            return
        print(f"  index ready={ready} — {msg}")
        time.sleep(20)
    print(f"[warn] index '{index_name}' not READY within timeout — first sync may still be running")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--endpoint-name", default="partssource-vs-endpoint")
    args, _ = parser.parse_known_args()

    w = WorkspaceClient()

    source_table = f"{args.catalog}.parts.parts_searchable"
    index_name = f"{args.catalog}.parts.parts_searchable_index"

    ensure_endpoint(w, args.endpoint_name)
    ensure_index(w, args.endpoint_name, index_name, source_table)

    print(f"\nVector Search ready:")
    print(f"  endpoint: {args.endpoint_name}")
    print(f"  index:    {index_name}")
    print(f"  source:   {source_table}")
    print(f"  embedding: {EMBEDDING_MODEL} (managed)")


if __name__ == "__main__":
    main()
