import uvicorn
import os
import yaml
import json
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import database
import psycopg2.extras
import recommendation_engine

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown
    database.close_db_pool()

app = FastAPI(lifespan=lifespan)

# Mount frontend directory for static files
if not os.path.exists("frontend"):
    os.makedirs("frontend")

app.mount("/static", StaticFiles(directory="frontend"), name="static")

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

class NoCacheStaticMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response

app.add_middleware(NoCacheStaticMiddleware)

class RecommendationRequest(BaseModel):
    system_id: str
    target_independence: int

@app.get("/")
async def read_index():
    return FileResponse(os.path.join("frontend", "index.html"))

@app.get("/api/systems")
def get_systems(search: str = ""):
    """
    Lookup systems by name, id, customer, or location.
    """
    conn = None
    try:
        conn = database.get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        query = """
            SELECT id, system_no, name, customer_name, location, state
            FROM systems
        """
        params = []
        if search:
            query += " WHERE (name ILIKE %s OR id::text ILIKE %s OR customer_name ILIKE %s OR location ILIKE %s)"
            search_param = f"%{search}%"
            params = [search_param] * 4

        query += " ORDER BY name ASC LIMIT 50"
        print(f"[DEBUG] SQL Query: {query}")
        print(f"[DEBUG] SQL Params: {params}")
        cur.execute(query, params)
        results = cur.fetchall()
        cur.close()
        return results
    except Exception as e:
        print(f"Error in lookup: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            database.release_db_connection(conn)

@app.get("/api/system-data/{system_id}")
def get_system_data(system_id: str):
    """
    Fetches system data directly from Postgres (via query_system) +
    reads any pre-existing CSV files. No slow subprocess calls.
    """
    try:
        data = recommendation_engine.gather_system_data(system_id)
        if not data.get("specs") and not data.get("summary_metrics"):
            raise HTTPException(status_code=404, detail="System not found — no Postgres data or CSV")
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/recommend")
def get_recommendation(request: RecommendationRequest):
    """
    Generates AI recommendation for a specific system and independence target.
    Always runs the AI for 100% independence; the frontend uses tier data to display
    the selected tier level.
    """
    try:
        data = recommendation_engine.gather_system_data(request.system_id)
        if not data.get("specs") and not data.get("summary_metrics"):
            raise HTTPException(status_code=404, detail="System not found")
        # Always compute for 100% so all tiers are consistent
        recommendation = recommendation_engine.get_ai_recommendation(data, 100)
        return recommendation
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("Starting Grid Independence Recommendation Service...")
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)