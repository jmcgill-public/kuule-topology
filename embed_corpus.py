#!/usr/bin/env python3
"""
Corpus Embedding Pipeline
Embeds documents (ADRs, configs, code) using EmbeddingGemma via Ollama
Stores in Postgres agent_corpus database with pgvector
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
import requests

# Configuration
OLLAMA_API = "http://localhost:11434/api/embeddings"
EMBED_MODEL = "embeddinggemma"
PG_CONN = "dbname=agent_corpus user=postgres host=/var/run/postgresql"

# Corpus sources (relative to ~/kafka/)
CORPUS_FILES = [
    ("session", "kafka.md"),
    ("session", "sql.md"),
    ("adr", "adr-001-high-fidelity-audit-consumer.md"),
    ("adr", "adr-002-durable-producer.md"),
    ("adr", "adr-003-broad-consumer-groups.md"),
    ("schema", "schema.sql"),
]

def get_embedding(text: str) -> list[float]:
    """Get embedding from Ollama EmbeddingGemma"""
    resp = requests.post(OLLAMA_API, json={
        "model": EMBED_MODEL,
        "prompt": text
    })
    resp.raise_for_status()
    return resp.json()["embedding"]

def embed_document(conn, doc_type: str, file_path: str, base_dir: Path):
    """Embed a single document and store in Postgres"""
    full_path = base_dir / file_path
    if not full_path.exists():
        print(f"⚠️  File not found: {full_path}")
        return False

    with open(full_path, 'r') as f:
        content = f.read()

    print(f"📄 Embedding {file_path} ({len(content)} chars)...")

    try:
        embedding = get_embedding(content)
        print(f"   ✓ Embedding generated ({len(embedding)} dims)")

        # Extract metadata
        metadata = {
            "bedrock_doc_id": str(full_path),
            "bedrock_source": f"file://{full_path}",
            "timestamp": datetime.now().isoformat()
        }

        custom_metadata = {
            "file_name": full_path.name,
            "file_size": len(content),
            "tags": [doc_type, "kafka", "event-bridge"]
        }

        # Insert into Postgres
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO documents (doc_type, file_path, content, embedding, metadata, custom_metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (
                doc_type,
                str(file_path),
                content,
                embedding,
                Json(metadata),
                Json(custom_metadata)
            ))
            result = cur.fetchone()
            if result:
                print(f"   ✓ Stored in Postgres (id: {result[0]})")
            else:
                print(f"   ⚠️  Document already exists (skipped)")
        conn.commit()
        return True

    except Exception as e:
        print(f"   ✗ Error: {e}")
        conn.rollback()
        return False

def main():
    base_dir = Path("/home/minix/kafka")

    print(f"🚀 Corpus Embedding Pipeline")
    print(f"   Base dir: {base_dir}")
    print(f"   Model: {EMBED_MODEL} via Ollama")
    print(f"   Database: agent_corpus")
    print()

    # Check Ollama is running
    try:
        resp = requests.get("http://localhost:11434/api/tags")
        models = [m["name"] for m in resp.json()["models"]]
        if EMBED_MODEL not in models:
            print(f"❌ Error: {EMBED_MODEL} not found in Ollama")
            print(f"   Available models: {', '.join(models)}")
            print(f"   Run: ollama pull {EMBED_MODEL}")
            return 1
    except requests.exceptions.ConnectionError:
        print("❌ Error: Ollama not running")
        print("   Start with: systemctl start ollama (or run ollama serve)")
        return 1

    # Connect to Postgres
    try:
        conn = psycopg2.connect(PG_CONN)
        print("✓ Connected to Postgres\n")
    except psycopg2.Error as e:
        print(f"❌ Postgres connection failed: {e}")
        return 1

    # Embed all corpus files
    success_count = 0
    for doc_type, file_path in CORPUS_FILES:
        if embed_document(conn, doc_type, file_path, base_dir):
            success_count += 1
        print()

    conn.close()

    print(f"✅ Pipeline complete: {success_count}/{len(CORPUS_FILES)} documents embedded")
    return 0 if success_count == len(CORPUS_FILES) else 1

if __name__ == "__main__":
    sys.exit(main())
