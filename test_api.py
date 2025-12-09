"""Quick test script for QKD pool endpoints"""
import requests
import json

BASE_URL = "http://127.0.0.1:8090"

def test_pool_status():
    print("Testing /qkd/keys/pool/status...")
    resp = requests.get(f"{BASE_URL}/qkd/keys/pool/status")
    print(f"Status: {resp.status_code}")
    print(f"Response: {json.dumps(resp.json(), indent=2)}")
    return resp.json()

def test_request_pool():
    print("\nTesting /qkd/keys/pool (POST)...")
    data = {
        "senderId": "alice@qumail.com",
        "receiverId": "bob@qumail.com",
        "count": 5
    }
    resp = requests.post(f"{BASE_URL}/qkd/keys/pool", json=data)
    print(f"Status: {resp.status_code}")
    result = resp.json()
    print(f"Response: {json.dumps(result, indent=2)}")
    return result

def test_pending_keys():
    print("\nTesting /qkd/keys/pending (GET)...")
    resp = requests.get(f"{BASE_URL}/qkd/keys/pending", params={"receiverId": "bob@qumail.com"})
    print(f"Status: {resp.status_code}")
    result = resp.json()
    print(f"Response: {json.dumps(result, indent=2)}")
    return result

def test_fetch_keys(key_ids):
    print("\nTesting /qkd/keys/fetch (POST)...")
    data = {
        "receiverId": "bob@qumail.com",
        "keyIds": key_ids[:2]  # Fetch first 2
    }
    resp = requests.post(f"{BASE_URL}/qkd/keys/fetch", json=data)
    print(f"Status: {resp.status_code}")
    result = resp.json()
    # Don't print full keyData
    for key in result.get("keys", []):
        key["keyData"] = key["keyData"][:50] + "..." if len(key.get("keyData", "")) > 50 else key.get("keyData")
    print(f"Response: {json.dumps(result, indent=2)}")
    return result

if __name__ == "__main__":
    print("=" * 50)
    print("QKD Pool API Test")
    print("=" * 50)
    
    try:
        test_pool_status()
        pool_result = test_request_pool()
        
        if pool_result.get("success"):
            pending_result = test_pending_keys()
            
            if pending_result.get("pendingKeyIds"):
                test_fetch_keys(pending_result["pendingKeyIds"])
        
        print("\n" + "=" * 50)
        print("All tests completed!")
        print("=" * 50)
    except requests.exceptions.ConnectionError:
        print("ERROR: Could not connect to server. Make sure it's running on port 8090")
