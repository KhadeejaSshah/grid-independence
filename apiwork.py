import requests
import yaml
from pathlib import Path
import os


# Load config
CONFIG_PATH = Path(__file__).resolve().parent / "conf.yaml"
try:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f) or {}
    giles = config.get("giles", {})
    apiwork_cfg = giles.get("apiwork", {})
    API_URL = apiwork_cfg.get("url") or os.getenv("GILES_API_URL")
    TOKEN = apiwork_cfg.get("token") or os.getenv("GILES_API_TOKEN")
except Exception as e:
    print(f"Warning: could not load conf.yaml: {e}")

# -------- FUNCTION --------
def get_system_details(system_id):
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "operationName": "systemDetails",
        "variables": {
            "systemId": system_id
        },
        "query": """
        query systemDetails($systemId: ID!) {
          systemV1(id: $systemId) {
            id
            imei
            latitude
            longitude
            cnic
            batteryModel
            hvNervesId
          }
          system(id: $systemId) {
            id
            orderId
            siteDetails {
              id
              tariff {
                id
                name
                type
                formattedName
                siteType
                parsedFromWeb
              }
              connectedLoad
              mf
              dcLimit
              billUrl
              phase
              meterType
              meterNumber
              billConsumerId
              isExportEnabled
              isNetMeteringActivated
              address
              billingDay
              connectionType
              referenceNumber
              refId
              plantId
              salesforceLink
              contractPower
              maxChargingPower
              maxDischargingPower
              vppConfigs
              timezoneOffset
              city {
                id
                name
                country
              }
              powerCompany
              feeder {
                id
                name
                powerCompanyId
              }
            }
          }
        }
        """
    }

    response = requests.post(API_URL, json=payload, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None


# -------- USAGE --------
if __name__ == "__main__":
    system_id = "c0aeb95e-033a-4c9d-8a49-35697de9df82"
    
    data = get_system_details(system_id)
    
    if data:
        print(data)