"""Editable pilot searches. Drafts have no sources until the owner selects them."""
import json
from engine import words
INTENT='ищу\nишу\nищем\nнужен\nнужна\nнужно\nнужны\nхочу\nхотим\nкуплю\nкупим\nсниму\nснять\nзабронировать\nзаказать\nпомогите\nкто занимается\nпосоветуйте\nпорекомендуйте\nподскажите\nкто может\nкто поможет\nу кого\nесть ли\nгде купить\nсколько стоит\nкакая цена\nlooking for\nneed\nwant\nrecommend\nanyone\nwhere can\nhow much'
EXCLUDED='предлагаем\nнаши услуги\nнаша компания\nнаш прайс\nподписывайтесь\nмы организуем\nмы предлагаем\nwe offer\nour services\nbook with us'
EXTRA_EXCLUDED={
 'cars':['стиральная машина','стиральную машину','стиральной машины','стиральные машины','посудомоечная машина','посудомоечную машину','швейная машина','швейную машину','washing machine','dishwasher','sewing machine'],
 'safari':['ищу девушек в тур','набираю группу','набираем группу','записывайтесь','отправлю программу и стоимость','специальная цена первого тура'],
}
TOPICS=[
 ('tickets','Билеты: UFC, концерты и шоу','билет\nбилеты\nбилета\nбилетов\nбилетик\nконцерт\nконцерта\nконцерты\nвыступление\nвыступления\nшоу\nufc\nюфс\nю эф си\nюфс\nстендап\nstandup\nstand up\nstand-up\nticket\ntickets\nconcert\nconcerts\nshow\ncoldplay\neminem\nконцерт артиста', 'Дубай билеты\nDubai tickets\nАбу Даби UFC\nDubai concerts'),
 ('yacht','Яхты: вечеринки и мероприятия','яхта\nяхту\nяхты\nяхте\nяхт\nаренда яхты\nкатер\nкатера\nкатере\nлодку\nyacht\nyachts\nboat\nboats\nyacht party\nboat party','Дубай яхты\nDubai yacht\nDubai events'),
 ('safari','Сафари в Дубае','сафари\nsafari\ndesert safari\nбагги\nbuggy\nквадроцикл\nквадроциклы\nквадроциклах\nдюны\nпоездка в пустыню\nтур в пустыню\nпустыню','Дубай экскурсии\nDubai safari\nDubai travel'),
 ('cars','Аренда автомобилей','аренда авто\nаренду авто\nаренда машины\nаренду машины\nмашину\nавтомобиль\nавто\nпрокат\nкабриолет\nламборгини\nlamborghini\nferrari\nферрари\nпорше\nporsche\nrent a car\ncar rental\nrental car\ncar hire','Дубай аренда авто\nDubai car rental\nДубай чат'),
 ('transfers','Трансферы и поездки','трансфер\nтрансфера\nтрансферы\nтрансфером\nтакси\nводитель\nводителя\nаэропорт\nаэропорта\nаэропорту\nвстретить в аэропорту\nотвезти\nдовезти\ntransfer\ntransfers\nairport pickup\nairport pick up\ndriver\nchauffeur','Дубай трансфер\nDubai airport\nАбу Даби чат'),
 ('hotels','Отели и бронирование со скидкой','отель\nотеля\nотеле\nотели\nгостиница\nгостиницу\nгостиницы\nбукинг\nbooking\nhotel\nhotels\nбронь отеля\nбронирование отеля\nпроживание\nапартаменты','Дубай отели\nDubai hotels\nДубай туристы'),
 ('visas','Визы и продление пребывания','виза\nвизу\nвизы\nвизе\nвизой\nвизами\nвизовый\nвизаран\nвиза ран\nvisa\nvisa run\nбордер ран\nborder run\nпродление пребывания\nрезидентство\nрезидентскую','Дубай визы\nDubai visa\nОАЭ релокация'),
 ('emirates_id','Emirates ID и резидентские документы','emirates id\nemiratesid\nemirates айди\nэмиратс айди\nэмиратс ид\nэмиратес айди\nэмиратес ид\nэмиратс id\nэмирейтс айди\nэмирейтс id\nрезидентская карта\nрезидентскую карту\nresidency id\nайди','Emirates ID\nДубай документы\nОАЭ резиденты'),
 ('smm','SMM и продвижение','smm\nсмм\nсммщика\nсммщик\nсмм специалист\nsmm специалист\nsmm-специалист\nсмм-специалист\nсмм-специалиста\nsmm-специалиста\nтаргетолог\nтаргетолога\nтаргет\nведение соцсетей\nвести соцсети\nведение инстаграм\nпродвижение\nконтент менеджер\nконтент-менеджер\nконтент-менеджера\nрилсмейкер\nрилсмейкера\nsocial media\nsocial media manager','Дубай бизнес\nDubai business\nДубай маркетинг')
]

def seed_topics(store):
    if store.get('topic_pack_v1'):return
    existing={r['name'] for r in store.rules()}
    with store.db:
        for key,name,keywords,queries in TOPICS:
            if name in existing:continue
            if store.db.execute('SELECT COUNT(*) FROM rules').fetchone()[0]>=10:break
            rule={'sources':[],'keywords':words(keywords),'required':words(INTENT),'excluded':words(EXCLUDED),'substring':False,'topic':key,'discovery_queries':queries.splitlines()}
            rule['excluded']+=words(EXTRA_EXCLUDED.get(key,[]))
            store.db.execute('INSERT INTO rules(name,data,enabled) VALUES(?,?,0)',(name,json.dumps(rule,ensure_ascii=False)))
    store.put('topic_pack_v1',True)
