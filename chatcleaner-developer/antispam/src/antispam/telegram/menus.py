from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def keyboard(rows):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=label,callback_data=data) for label,data in row] for row in rows])


def home():
    markup = keyboard([
        [('Группы','groups'),('Проверить текст','prompt:check:0')],
        [('Общие правила','rules:0'),('Общие исключения','exceptions:0')],
        [('Доверенные','users:trusted:0'),('Блокировки','users:blocked:0')],
        [('На разбор','review:0'),('Журнал','log:0')],
        [('Статистика','stats:0'),('Настройки','settings:0')],
        [('Статус','status'),('Отмена ввода','cancel')],
        [('Инструкция для теста','guide'),('Словарь и исключения','dictionary')],
        [('Выгрузить настройки','export')]
    ])
    markup.inline_keyboard.insert(0,[InlineKeyboardButton(text='Подключить тестовую группу',url='https://t.me/ExampleModerationBot?startgroup=pilot')])
    return markup


def chat_menu(chat):
    cid=chat['chat_id']
    return keyboard([
        [('Проверить текст',f'prompt:check:{cid}'),('Проверить права',f'rights:{cid}')],
        [('Правила',f'rules:{cid}'),('Исключения',f'exceptions:{cid}')],
        [('Доверенные',f'users:trusted:{cid}'),('Блокировки',f'users:blocked:{cid}')],
        [('На разбор',f'review:{cid}'),('Журнал',f'log:{cid}')],
        [('Статистика',f'stats:{cid}'),('Настройки',f'settings:{cid}')],
        [('Режим наблюдения',f'mode:{cid}:observe'),('Включить удаление',f'mode:{cid}:active')],
        [('Остановить проверки',f'mode:{cid}:off'),('Главное меню','home')]
    ])
