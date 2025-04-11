import os, json
import requests
KEEPA_KEY = os.getenv("KEEPA_KEY")
OPENAI_KEY = os.getenv("OPENAI_KEY")

ASIN = "B00NLLUMOE"

# keepa_base_url = "https://api.keepa.com"
# keepa_endpoint = f"/product?key={KEEPA_KEY}&domain=1&asin={ASIN}"

openai_base_url = "https://api.openai.com/v1"
openai_endpoint = "/responses"
openai_headers = {"Content-Type": "application/json", "Authorization": f"Bearer {OPENAI_KEY}"} 
openai_payload = {
    "model": "gpt-4o-mini",
    "input": "I'm good. What's my name again?",
    "tool_choice": "auto",
    "tools":[{ "type": "web_search_preview",
              "user_location": {
                  "type": "approximate"},
                  "search_context_size": "medium"}]
  }

# print(openai_payload, '\n')
# print(json.dumps(openai_payload, indent=2), '\n')

def calculate_price(response):
    input_tokens = response.json()['usage']['input_tokens']
    output_tokens = response.json()['usage']['output_tokens']
    return input_tokens/1000000*0.15 + output_tokens/1000000*0.60


# #keepa
# response = requests.post(url=keepa_base_url+keepa_endpoint)
# print(f'Response successful: {response.ok}\nResponse status: {response.status_code}')#\nTokens left: {response.json()["tokensLeft"]}\n')
# print(response.json()['products'][0].keys(), '\n')
# print(response.json()['products'][0]['monthlySold'])

# openai
response = requests.post(url=openai_base_url+openai_endpoint, headers=openai_headers, data=json.dumps(openai_payload))
# print(f'Response successful: {response.ok}\nResponse status: {response.status_code}\n{response.text}\n')
print(response.json()['output'][0]['content'][0]['text'])
print(f'Spent: ${calculate_price(response)}')

