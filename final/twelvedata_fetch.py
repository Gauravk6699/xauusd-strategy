import requests
import json

# Define API details
API_URL = "https://api.twelvedata.com/time_series"
API_KEY = "71b36f5f96a2489d8454c4a1f2da621e"
SYMBOL = "XAU/USD"
INTERVAL = "5min"
OUTPUT_SIZE = 10

# Construct the request parameters
params = {
    "symbol": SYMBOL,
    "interval": INTERVAL,
    "apikey": API_KEY,
    "outputsize": OUTPUT_SIZE
}

print(f"Requesting time series data for {SYMBOL} with interval {INTERVAL} from Twelve Data API...\n")

try:
    # Make the GET request
    response = requests.get(API_URL, params=params)
    response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

    # Parse the JSON response
    try:
        data = response.json()

        # Print the full JSON response
        print("Full JSON Response:")
        print(json.dumps(data, indent=4))
        print("-" * 50)

        # Check if the expected keys 'meta' and 'values' are in the response
        if 'meta' in data and 'values' in data:
            # Print the meta part
            print("\nMeta Information:")
            print(json.dumps(data['meta'], indent=4))
            print("-" * 50)

            # Print the first 2-3 data points from the values array
            print("\nFirst 2-3 Data Points (OHLCV):")
            values = data['values']
            for i, point in enumerate(values[:3]): # Print up to the first 3 points
                if i < OUTPUT_SIZE: # ensure we don't go out of bounds if less data than 3
                    print(f"Data Point {i+1}: {json.dumps(point, indent=2)}")
                else:
                    break
            print("-" * 50)

            # Confirmation if 'volume' is present in the data points
            if values and isinstance(values[0], dict) and 'volume' in values[0]:
                print("\nConfirmation: 'volume' field is present in the data points.")
            elif values and isinstance(values[0], dict):
                print("\nConfirmation: 'volume' field is NOT present in the data points.")
            else:
                print("\nCould not determine presence of 'volume' field due to missing or malformed 'values' data.")
            print("-" * 50)

        elif 'message' in data and 'code' in data and data['code'] != 200 : # Typical error structure for Twelve Data
            print(f"\nAPI Error Received:")
            print(f"Code: {data.get('code')}")
            print(f"Message: {data.get('message')}")
            if 'details' in data:
                 print(f"Details: {json.dumps(data['details'], indent=4)}")
            print("-" * 50)
        else:
            print("\nUnexpected JSON structure received. 'meta' and/or 'values' keys are missing, and no standard API error format detected.")
            print("Please inspect the full JSON response above.")
            print("-" * 50)

    except json.JSONDecodeError:
        print("\nError: Failed to decode JSON response from the server.")
        print(f"Response text: {response.text}")
        print("-" * 50)

except requests.exceptions.HTTPError as http_err:
    print(f"\nHTTP error occurred: {http_err}")
    print(f"Response Content: {response.content.decode() if response.content else 'No content'}")
    print("-" * 50)
except requests.exceptions.RequestException as req_err:
    print(f"\nRequest error occurred: {req_err}")
    print("-" * 50)
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}")
    print("-" * 50)

print("\nScript execution finished.")
