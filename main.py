from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import os
import asyncpg
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
# In production, environment variables should be set by the deployment platform
load_dotenv()

app = FastAPI(title="XO User Memory API")
pool: Optional[asyncpg.Pool] = None
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError(
        "DATABASE_URL environment variable is not set. "
        "Please set it in your .env file (for local development) or "
        "configure it in your deployment platform's environment variables settings."
    )


# Models
class MemoryCreate(BaseModel):
    channel: str
    peer: str
    fact: str
    category: str = "general"
    session_id: Optional[str] = None
    confidence: float = Field(default=1.0, ge=0, le=1)
    expires_at: Optional[datetime] = None


class MemoryOut(BaseModel):
    id: int
    fact: str
    category: str
    confidence: float
    created_at: datetime
    expires_at: Optional[datetime] = None


class MemoryListResponse(BaseModel):
    channel: str
    peer: str
    user_id: Optional[str]
    xo_user_id: Optional[str]
    memories: List[MemoryOut]
    total: int


class BulkMemoryCreate(BaseModel):
    channel: str
    peer: str
    session_id: Optional[str] = None
    memories: List[dict]


# Lifecycle
@app.on_event("startup")
async def startup():
    global pool
    try:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        print(f"Database connection pool created successfully")
    except Exception as e:
        print(f"Error creating database pool: {e}")
        raise


@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()


# Helper: get or create user
async def get_or_create_user(conn, channel: str, peer: str):
    """Get user_id from users table, create if not exists."""
    row = await conn.fetchrow(
        "SELECT id, xo_user_id FROM users WHERE channel = $1 AND channel_peer = $2",
        channel, peer
    )
    if row:
        return row["id"], row["xo_user_id"]
    
    # Create new user
    row = await conn.fetchrow("""
        INSERT INTO users (id, channel, channel_peer, label, status, created_at, updated_at)
        VALUES (gen_random_uuid(), $1, $2, $2, 'active', NOW(), NOW())
        RETURNING id
    """, channel, peer)
    return row["id"], None


# Endpoints
@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "XO User Memory API",
        "health": "/health",
        "docs": "/docs"
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/user-memories", status_code=201)
async def create_memory(mem: MemoryCreate):
    async with pool.acquire() as conn:
        user_id, _ = await get_or_create_user(conn, mem.channel, mem.peer)
        
        try:
            row = await conn.fetchrow("""
                INSERT INTO user_memories 
                    (user_id, fact, category, session_id, confidence, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, fact, category, confidence, created_at, expires_at
            """, user_id, mem.fact, mem.category, mem.session_id, 
                mem.confidence, mem.expires_at)
            return dict(row)
        except asyncpg.UniqueViolationError:
            existing = await conn.fetchrow(
                "SELECT id FROM user_memories WHERE user_id = $1 AND fact = $2",
                user_id, mem.fact)
            raise HTTPException(409, {"message": "Memory exists", "existing_id": existing["id"]})


@app.get("/user-memories", response_model=MemoryListResponse)
async def get_memories(
    channel: str = Query(...),
    peer: str = Query(...),
    category: Optional[str] = None,
    limit: int = Query(default=50, le=200),
    since: Optional[datetime] = None
):
    async with pool.acquire() as conn:
        # Get user
        user_row = await conn.fetchrow(
            "SELECT id, xo_user_id FROM users WHERE channel = $1 AND channel_peer = $2",
            channel, peer
        )
        
        if not user_row:
            return {
                "channel": channel, "peer": peer, 
                "user_id": None, "xo_user_id": None,
                "memories": [], "total": 0
            }
        
        user_id = user_row["id"]
        xo_user_id = user_row["xo_user_id"]
        
        # Build query
        query = """
            SELECT id, fact, category, confidence, created_at, expires_at
            FROM user_memories
            WHERE user_id = $1
              AND (expires_at IS NULL OR expires_at > NOW())
        """
        params = [user_id]
        idx = 2
        
        if category:
            query += f" AND category = ${idx}"
            params.append(category)
            idx += 1
        if since:
            query += f" AND created_at > ${idx}"
            params.append(since)
            idx += 1
        
        query += f" ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)
        
        rows = await conn.fetch(query, *params)
        memories = [dict(r) for r in rows]
        
        return {
            "channel": channel, "peer": peer,
            "user_id": str(user_id), "xo_user_id": xo_user_id,
            "memories": memories, "total": len(memories)
        }


@app.delete("/user-memories/{memory_id}")
async def delete_memory(memory_id: int):
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM user_memories WHERE id = $1", memory_id)
        if result == "DELETE 0":
            raise HTTPException(404, "Memory not found")
        return {"ok": True, "deleted_id": memory_id}


@app.post("/user-memories/bulk", status_code=201)
async def create_bulk(bulk: BulkMemoryCreate):
    async with pool.acquire() as conn:
        user_id, _ = await get_or_create_user(conn, bulk.channel, bulk.peer)
        
        created = duplicates = 0
        for m in bulk.memories:
            try:
                await conn.execute("""
                    INSERT INTO user_memories 
                        (user_id, fact, category, session_id, confidence)
                    VALUES ($1, $2, $3, $4, $5)
                """, user_id, m.get("fact"), m.get("category", "general"),
                    bulk.session_id, m.get("confidence", 1.0))
                created += 1
            except asyncpg.UniqueViolationError:
                duplicates += 1
        
        return {"created": created, "duplicates_skipped": duplicates}


@app.delete("/user-memories")
async def delete_all(channel: str = Query(...), peer: str = Query(...)):
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            "SELECT id FROM users WHERE channel = $1 AND channel_peer = $2",
            channel, peer
        )
        if not user_row:
            return {"ok": True, "deleted_count": 0}

        result = await conn.execute(
            "DELETE FROM user_memories WHERE user_id = $1", user_row["id"]
        )
        return {"ok": True, "deleted_count": int(result.split()[-1])}


# Vercel serverless handler
from mangum import Mangum
handler = Mangum(app)