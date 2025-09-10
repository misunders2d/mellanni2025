import openai
import os

OPENAI_KEY = os.getenv("OPENAI_KEY")

bot = openai.OpenAI(api_key=OPENAI_KEY)

# text_res = bot.chat.completions.create(

#     model="gpt-4o-mini",
#     messages=[
#         {
#             "role": "system",
#             "content": "You are Grok, a highly intelligent, helpful AI assistant."
#         },
#         {
#             "role": "user",
#             "content": "What is the meaning of life, the universe, and everything?"
#         },
#     ],

# )

img_res = bot.images.generate(
    prompt="A futuristic city skyline at sunset, with flying cars and neon lights",
    model="dall-e-3",
)

print(img_res.data[0].url)
