import requests

api_url = "https://cloud.delman.io/analytic/api/data?publish_id=1e99f3c4-c401-4df6-8206-6a82ddd4dda6&page=0&page_size=25"
headers = {"authorization": "a0741beb99f35c8a163cd13988a2d3b8"}
response = requests.get(api_url, headers=headers)

# get data
print(response.json())

# get status code
print(response.status_code)
