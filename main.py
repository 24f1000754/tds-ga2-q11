
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any
import time
import base64
import json
import hashlib
import threading

app = FastAPI()

# Allow browser-based grader requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

# Assigned values from the question
TOTAL_ORDERS = 55
RATE_LIMIT = 17
WINDOW_SECONDS = 10

# In-memory state
orders = {}
idempotency_keys = {}
client_buckets = {}
lock = threading.Lock()


class OrderRequest(BaseModel):
    model_config = {"extra": "allow"}


def make_cursor(offset: int) -> str:
    raw = json.dumps(
        {"offset": offset},
        separators=(",", ":")
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def read_cursor(cursor: str) -> int:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded)
        data = json.loads(raw)
        offset = data["offset"]

        if type(offset) is not int:
            raise ValueError()

        if offset < 0 or offset > TOTAL_ORDERS:
            raise ValueError()

        return offset
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid cursor"
        )


@app.middleware("http")
async def per_client_rate_limit(request: Request, call_next):
    # CORS preflight requests must be allowed
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("x-client-id", "anonymous")
    now = time.monotonic()

    with lock:
        start, count = client_buckets.get(
            client_id, (now, 0)
        )

        if now - start >= WINDOW_SECONDS:
            start, count = now, 0

        if count >= RATE_LIMIT:
            retry_after = max(
                1,
                int(WINDOW_SECONDS - (now - start) + 0.999)
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        client_buckets[client_id] = (start, count + 1)

    return await call_next(request)


@app.post("/orders", status_code=201)
async def create_order(
    request: Request,
    idempotency_key: str = Header(
        ..., alias="Idempotency-Key"
    ),
):
    if not idempotency_key.strip():
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key cannot be empty"
        )

    body = await request.body()
    fingerprint = hashlib.sha256(body).hexdigest()

    with lock:
        existing = idempotency_keys.get(idempotency_key)

        if existing:
            old_fingerprint, order_id = existing

            if old_fingerprint != fingerprint:
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency key reused with different data"
                )

            return JSONResponse(
                status_code=200,
                content={"id": order_id}
            )

        order_id = len(orders) + 1
        orders[order_id] = {
            "id": order_id,
            "data": json.loads(body) if body else {},
        }
        idempotency_keys[idempotency_key] = (
            fingerprint, order_id
        )

    return JSONResponse(
        status_code=201,
        content={"id": order_id}
    )


@app.get("/orders")
def list_orders(
    limit: int = 10,
    cursor: str | None = None,
):
    if limit < 1:
        raise HTTPException(
            status_code=400,
            detail="limit must be positive"
        )

    offset = read_cursor(cursor) if cursor else 0

    # Fixed catalog: IDs 1 through 55
    page_ids = list(
        range(offset + 1, min(offset + limit, TOTAL_ORDERS) + 1)
    )

    items = [{"id": order_id} for order_id in page_ids]
    next_offset = offset + len(items)

    next_cursor = (
        make_cursor(next_offset)
        if next_offset < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.get("/")
def home():
    return {"message": "Orders API is running"}
