import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import database

app = FastAPI()

class SystemDetailsRequest(BaseModel):
    system_id: str

@app.post("/GetSystemDetails")
def get_system_details(request: SystemDetailsRequest):
    """
    API to get system details from the database.
    """
    conn = None
    try:
        conn = database.get_db_connection()
        cursor = conn.cursor()
        query = """
SELECT id, is_export_enabled, deployed_at, disconnected
FROM systems
WHERE id = %s
  AND is_export_enabled = true
  AND disconnected = false
  AND deployed_at < %s;
"""
        cursor.execute(query, (request.system_id, '2024-01-01'))
        result = cursor.fetchone()

        if result:
            columns = [desc[0] for desc in cursor.description]   # do this BEFORE cursor.close()
            system_details = dict(zip(columns, result))
            cursor.close()
            return {"system_details": system_details}
        cursor.close()

        raise HTTPException(status_code=404, detail="System not found")

    except Exception as e:
        # Log the error in a real application
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    finally:
        if conn:
            database.release_db_connection(conn)

@app.on_event("shutdown")
def shutdown_event():
    database.close_db_pool()

if __name__ == "__main__":
    print("Starting System Details Service...")
    print("To use the API, send a POST request to http://127.0.0.1:8000/GetSystemDetails")
    print("Request body example: {\"system_id\": \"your_system_id\"}")
    uvicorn.run(app, host="127.0.0.1", port=8000)