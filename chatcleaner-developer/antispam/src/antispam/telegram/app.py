import asyncio
import json
import logging
import os
import fcntl
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

from antispam.config import Settings
from antispam.storage.repositories import Store
from antispam.telegram.handlers import Controller, env_file


async def run():
    if Path('data/MIGRATED_TO_SERVER').exists():
        raise RuntimeError('Bot migrated to Google VM. Do not start a second polling instance. See deploy documentation.')
    Path('data').mkdir(exist_ok=True)
    lock=open('data/bot.lock','a+')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close();raise RuntimeError('A local bot instance is already running.')
    lock.seek(0);lock.truncate();lock.write(str(os.getpid()));lock.flush()
    settings=Settings.from_env({**env_file(),**os.environ})
    path=settings.policy_path
    if not path.exists():path=Path('config/policy.example.json')
    policy=json.loads(path.read_text())
    store=Store(settings.database_path)
    bot=Bot(settings.bot_token,default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    controller=Controller(bot,store,settings,policy)
    dp=Dispatcher();dp.include_router(controller.router)
    tasks=[]
    try:
        me=await bot.get_me();info=await bot.get_webhook_info()
        if info.url:raise RuntimeError('An existing webhook is configured; stop the previous installation first.')
        await bot.set_my_commands([
            BotCommand(command='start',description='Открыть бота'),
            BotCommand(command='menu',description='Управление модерацией'),
            BotCommand(command='id',description='Узнать свой Telegram ID'),
            BotCommand(command='check',description='Проверить сообщение без наказания'),
            BotCommand(command='cancel',description='Отменить ввод')
        ])
        logging.getLogger(__name__).info('Connected to @%s; owners=%s',me.username,len(controller.owners()))
        tasks.append(asyncio.create_task(controller.executor.retry_loop()))
        async def maintenance():
            while True:store.cleanup();await asyncio.sleep(300)
        tasks.append(asyncio.create_task(maintenance()))
        await dp.start_polling(bot,allowed_updates=['message','edited_message','my_chat_member','chat_member','callback_query'],handle_as_tasks=False)
    finally:
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await bot.session.close();store.close();lock.close()


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s %(message)s')
    # Never enable HTTP request debug logs containing Bot API credentials.
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    secret=env_file().get('TELEGRAM_BOT_TOKEN','')
    class Redact(logging.Filter):
        def filter(self,record):
            if secret:
                record.msg=record.getMessage().replace(secret,'[REDACTED]');record.args=()
            return True
    for handler in logging.getLogger().handlers:handler.addFilter(Redact())
    try:asyncio.run(run())
    except KeyboardInterrupt:pass


if __name__=='__main__':main()
