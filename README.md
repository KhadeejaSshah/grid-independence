# System Details Service

This service provides an API endpoint to get system details from the PostgreSQL database.

## Setup

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure the database connection in `conf.yaml`.

3. Run the service:
   ```bash
   python main.py
   ```

## API

- **Endpoint:** `POST /get_system_details`
- **Request Body:**
  ```json
  {
    "system_id": "your_system_id"
  }
  ```
- **Response:**
  ```json
  {
    "system_details": {
      "id": "your_system_id",
      "data": "..."
    }
  }
  ```

source .venv/bin/activate
to run:  
python3 query_system.py --system-id c0aeb95e-033a-4c9d-8a49-35697de9df82