"""Customer requests for event sound equipment, separate from DJ performers."""
from engine import words
from dj_topic import DJ_INTENT

SOUND_NAME = 'Звук и аппаратура: колонки и оборудование для мероприятий'
KEYWORDS = '''аппаратура
аппаратуру
аппаратуры
колонки
колонку
колонок
звуковое оборудование
звукового оборудования
музыкальное оборудование
музыкального оборудования
звук
звука
озвучка
озвучку
озвучивание
акустическая система
акустическую систему
акустические системы
акустических систем
микрофон
микрофоны
микрофона
микрофонов
сабвуфер
сабвуферы
микшер
микшерный пульт
диджейское оборудование
dj оборудование
dj equipment
dj controller
sound equipment
sound system
sound systems
audio equipment
pa system
pa systems
speakers
speaker rental
speaker hire
microphone
microphones
subwoofer
subwoofers
audio mixer'''
EXCLUDED = '''предлагаю
предлагаем
сдаю
сдаем
сдам
сдадим
продаю
продаем
продам
наши услуги
мои услуги
у нас в аренду
пишите в личку для заказа
ищу работу
ищу подработку
ищу заказы
вакансия
вакансии
зарплата
оклад
в штат
на постоянную работу
резюме
we offer
we rent out
we provide
our services
book with us
available for rent
for sale
looking for work
looking for a job
job vacancy
salary
full time
full-time
газовая колонка
газовую колонку
газовые колонки
газовых колонок
водонагреватель
медицинская аппаратура
медицинскую аппаратуру
слуховой аппарат
слуховые аппараты
стиральная
на ноутбуке
в ноутбуке
на телефоне
в телефоне
пропал звук
звук пропал
звук не работает
ремонт колонок
починить колонку
спикера
спикер
motivational speaker
keynote speaker
speakers for conference
speakers for a conference
computer speakers
car speakers
gas heater
hearing aid'''


def sound_rule(sources=()):
    return dict(sources=list(dict.fromkeys(sources)), keywords=words(KEYWORDS),
                required=words(DJ_INTENT+'\nарендовать\nвзять напрокат\nснять\nlooking to rent\nrent a\nrent speakers\nhire speakers'),
                excluded=words(EXCLUDED), substring=False, demand_only=True,
                topic='sound_equipment', discovery_queries=['Дубай мероприятия','Dubai events','Dubai party','Дубай свадьба','Dubai wedding','Dubai sound rental','Дубай аренда звука'])
