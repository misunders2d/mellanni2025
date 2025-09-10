import os, json, textwrap
import asyncio

from openai import OpenAI, NotFoundError
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import BadRequest

TG_KEY = "tg_api_key"
OPENAI_KEY = "openai_api_key"
client = OpenAI(api_key=OPENAI_KEY)


class Bot:
    def __init__(self, token=TG_KEY):
        self.app = Application.builder().token(token).build()
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("version", self.version))
        self.app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.text_response)
        )

    async def text_response(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_chat_action("typing")
        prompt = update.message.text
        res = client.chat.completions.create(
            model="gpt-3.5-turbo", messages=[{"role": "user", "content": prompt}]
        )
        response = res.choices[0].message.content
        await update.message.reply_text(response)

    async def placeholder(self, *args, **kwargs):
        print(args)
        print(kwargs)
        pass

    async def version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Version 2.0.2")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Hello! I'm your bot. How can I assist you?")

    async def on_shutdown(self, *args):
        print("Bot is shutting down...")
        print("Sessions saved.")

    def run(self):
        print("Bot running...")
        self.app.post_shutdown = self.on_shutdown
        self.app.run_polling()


if __name__ == "__main__":
    bot_app = Bot()
    bot_app.run()
