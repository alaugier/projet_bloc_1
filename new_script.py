
import requests

response = requests.get("url",
    headers={
        "Authorization": "Bearer isDisconnected",
        "Content-Type": "application/json"
    },
    cookies={},
    auth=(),
)

if response.status_code == 200:
    print(response.content)
else:
    print(f'Failed to retrieve the page. Status code: {response.status_code}')
