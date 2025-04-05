import json
import requests
import os

def lambda_handler(event, context):
    print("🔔 Event received:", json.dumps(event))

    # Salesforce environment variables (set in Lambda console or via Secrets Manager)
    sf_instance_url = os.environ["SF_INSTANCE_URL"]  # e.g. https://your_instance.salesforce.com
    access_token = os.environ["SF_ACCESS_TOKEN"]

    # Salesforce REST endpoint for inserting Account
    account_url = f"{sf_instance_url}/services/data/v58.0/sobjects/Account"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Extract record from EventBridge Pipe event
    record = event["records"][0]["value"]

    # Example record: {"name": "Acme Corp", "industry": "Technology", "phone": "1234567890"}

    # Prepare payload — rename keys if necessary to match Salesforce field names
    payload = {
        "Name": record.get("name"),
        "Industry": record.get("industry"),
        "Phone": record.get("phone")
    }

    # Send POST request to Salesforce REST API
    response = requests.post(account_url, headers=headers, json=payload)

    if response.status_code == 201:
        result = response.json()
        print(f"✅ Account created. ID: {result['id']}")
        return {"status": "success", "salesforce_id": result["id"]}
    else:
        print(f"❌ Failed to create Account. Response: {response.text}")
        return {
            "status": "error",
            "response_code": response.status_code,
            "message": response.text
        }
