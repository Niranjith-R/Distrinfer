import requests



url = "http://127.0.0.1:8000/query"
count = 0
with open("payload.txt", "r") as file:
    content = file.readlines()
    for i in content:
        if count == 1000:
            break
        payload = { 
            "prompt" : i,
            "UID" : 1
        }
        count += 1
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            print(response.status_code)
            print(response.json())

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")

        except Exception as err:
            print(f"An error occurred: {err}")
