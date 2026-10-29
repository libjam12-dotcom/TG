"""Client-only DJ search preset. Installing it never changes personal searches."""
from engine import words

DJ_NAME = 'Диджей / DJ: вечеринки, яхты и мероприятия'
DJ_KEYWORDS = '''диджей
диджея
диджею
диджеем
диджеи
диджеев
диджейка
диджейку
диджейки
диджейский
диджейское
диджейские
диджеинг
диджеинга
диджейинг
диджей на яхту
диджей на свадьбу
диджей на корпоратив
ди джей
ди джея
ди-джей
ди-джея
диджей-девушка
dj
djs
deejay
disc jockey
disk jockey
djane
dj set
dj for yacht
wedding dj
party dj
event dj'''
DJ_INTENT = '''ищу
ишу
ищем
нужен
нужна
нужно
нужны
требуется
требуются
разыскиваем
посоветуйте
порекомендуйте
подскажите
можете порекомендовать
можете посоветовать
кто знает
кто может
кто сможет
у кого есть
есть ли
контакт
контакты
контактом
контактами
заказать
пригласить
нанять
забронировать
сколько стоит
какая стоимость
какая цена
looking for
looking to hire
need
needed
require
wanted
recommend
recommendation
recommendations
anyone know
anyone available
any dj
any djs
available dj
available djs
book a dj
hire a dj
dj hire
dj booking
how much'''
DJ_EXCLUDED = '''предлагаю услуги
предлагаем услуги
наши услуги
мои услуги
услуги диджея
услуги dj
я диджей
я dj
я профессиональный диджей
я профессиональный dj
ищу работу
ищу заказы
ищу заказчиков
ищем заказы
ищу площадку
ищу площадки
ищу резиденцию
ищу резидентство
ищу выступления
открыт к сотрудничеству
открыта к сотрудничеству
принимаю заказы
принимаем заказы
заказывайте
забронируйте меня
обучение диджеингу
курсы диджеинга
школа диджеев
уроки диджеинга
диджейское оборудование
dj оборудование
dj equipment
dj controller
dj controllers
dj mixer
dj decks
контроллер
контроллеры
контроллера
пульт
пульта
пульты
проигрыватель
проигрыватели
консоль
консоли
курс
курсы
урок
уроки
обучение
научиться
трек
треки
плейлист
запись сета
сет послушать
looking for work
looking for gigs
looking for bookings
looking for venues
available for bookings
available for booking
book me
our services
we offer
i am a dj
i'm a dj
dj course
dj courses
dj lessons
learn to dj'''
DJ_QUERIES = ['Дубай мероприятия', 'Dubai events', 'Дубай вечеринки', 'Dubai party',
              'Дубай свадьба', 'Dubai wedding', 'Дубай яхты', 'Dubai yacht party']


def dj_rule(sources=()):
    # Self-promotion phrase «услуги диджея» is also used by customers, so it must
    # stay a keyword, not a blanket exclusion.
    excluded = [x for x in words(DJ_EXCLUDED) if x not in ('услуги диджея', 'услуги dj')]
    return dict(sources=list(dict.fromkeys(sources)), keywords=words(DJ_KEYWORDS),
                required=words(DJ_INTENT), excluded=excluded, substring=False,
                topic='dj', discovery_queries=DJ_QUERIES)
