import asyncio
import logging
from time import sleep

from telethon import TelegramClient
from telethon.tl.types import InputPeerChat, User, Chat, Channel

bot: TelegramClient
alert_chat: [User | Chat | Channel] = None

logger = logging.getLogger(__name__)


async def initialize_bot(tele_config: dict):
    global bot, alert_chat
    logger.info("Initializing Alert bot")
    bot = TelegramClient('bot',
                         tele_config["api_id"],
                         tele_config["api_hash"])

    bot_config = tele_config['bot']
    bot = await bot.start(bot_token=bot_config['token'])
    alert_chat = await bot.get_entity(bot_config['chatId'])
    await send_alert("Initializing application")


async def send_alert(message: str, level: int = None):
    global bot, alert_chat
    if not level:
        level = logging.INFO

    while alert_chat is None:
        await asyncio.sleep(0.1)

    if level in [logging.WARN, logging.ERROR]:
        message = '🛑 ' + message
    logger.log(level, message)
    await bot.send_message(alert_chat, message)
