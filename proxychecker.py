import requests

url = 'https://ipv4.icanhazip.com'
proxy = 'geo.iproyal.com:12321'
proxy_auth = 'OLCTWR4ykDXBI2H4:ogUqTjfLuXvm5JGo_country-us_state-california_streaming-1'
proxies = {
   'http': f'http://{proxy_auth}@{proxy}',
   'https': f'http://{proxy_auth}@{proxy}'
}

response = requests.get(url, proxies=proxies)
print(response.text)