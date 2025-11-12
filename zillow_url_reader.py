import urllib.parse
import json
import re
import pprint

def decode_json_from_url(url: str):
    # Extract all key=value pairs from the query string
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)

    for key, values in query_params.items():
        value = values[0]
        # Try to find and decode any JSON-like content
        if re.search(r'%7B.*%7D', value):
            decoded = urllib.parse.unquote(value)
            try:
                data = json.loads(decoded)
                print(f"\n🔍 Decoded JSON for '{key}':")
                pprint.pprint(data)
            except json.JSONDecodeError:
                print(f"\nCould not parse JSON for '{key}', raw decoded:")
                print(decoded)
        else:
            print(f"\n'{key}' = {value}")

url = input("Paste the full URL here:\n> ").strip()


decode_json_from_url(url)

# Pretty print the decoded JSON for the user
parsed_url = urllib.parse.urlparse(url)
query_params = urllib.parse.parse_qs(parsed_url.query)

for key, values in query_params.items():
    for value in values:
        # Attempt to pretty print JSON-like values
        decoded = urllib.parse.unquote(value)
        try:
            data = json.loads(decoded)
            print(f"\nPretty printed JSON for '{key}':")
            pprint.pprint(data)
        except json.JSONDecodeError:
            continue
