-- agent_corpus database schema
-- Stores embeddings of ADRs, configs, code for specialized AI agents

CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_type TEXT NOT NULL,              -- 'adr', 'config', 'code', 'schema', 'session'
    file_path TEXT NOT NULL,             -- Source file path
    content TEXT NOT NULL,               -- Full document text
    embedding vector(768),               -- EmbeddingGemma output (768 dimensions)
    metadata JSONB,                      -- Bedrock-compatible metadata
    custom_metadata JSONB,               -- Domain-specific (language, framework, tags)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for vector similarity search
CREATE INDEX IF NOT EXISTS documents_embedding_idx
    ON documents USING ivfflat (embedding vector_cosine_ops);

-- Indexes for metadata filtering
CREATE INDEX IF NOT EXISTS documents_custom_metadata_idx
    ON documents USING gin (custom_metadata);

CREATE INDEX IF NOT EXISTS documents_doc_type_idx
    ON documents (doc_type);

-- Agent registry table
CREATE TABLE IF NOT EXISTS agents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,           -- kraft-config-agent, cloudevents-schema-agent
    domain TEXT NOT NULL,                -- kafka, postgres, aws, etc.
    capability TEXT NOT NULL,            -- config-generation, schema-validation, etc.
    model TEXT NOT NULL,                 -- qwen-14b, claude-sonnet-4, etc.
    corpus_filter JSONB,                 -- Which doc_types to retrieve
    prompt_template TEXT NOT NULL,       -- How to construct prompts
    created_at TIMESTAMP DEFAULT NOW()
);

-- Embedding job tracking (for pipeline)
CREATE TABLE IF NOT EXISTS embedding_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_path TEXT NOT NULL,
    status TEXT NOT NULL,                -- 'pending', 'processing', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS embedding_jobs_status_idx
    ON embedding_jobs (status);
