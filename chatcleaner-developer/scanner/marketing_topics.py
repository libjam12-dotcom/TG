"""Personal SignM demand searches. Never import a customer's configuration."""
import json
from engine import words

INTENT = '''ищу
ишу
ищем
нужен
нужна
нужно
нужны
требуется
хочу
хотим
посоветуйте
порекомендуйте
кто может
кто занимается
кто поможет
подскажите
сколько стоит
заказать'''

EXCLUDED = '''предлагаю услуги
предлагаем услуги
наши услуги
наша компания
наш прайс
подписывайтесь
разработаю
разрабатываем сайты
создаю сайты
создаем сайты
делаем сайты
сделаю сайт
настрою рекламу
мы предлагаем
ищу клиентов
ищем клиентов
ищу заказы
ищу работу
ищу подработку
в поиске работы
резюме
полный рабочий день
в штат
we offer
our services
i build websites
looking for clients
looking for a job
my services
веду соцсети
ведем соцсети
пишите в лс за подробностями'''

COMMON_QUERIES = 'Фриланс заказы\nФриланс чат\nПредприниматели чат'

# Topic name, literal phrases, focused discovery queries and editable first replies.
TOPICS = [
    ('websites', 'Сайты и лендинги',
     'нужен сайт\nнужен лендинг\nсделать сайт\nсоздать сайт\nразработать сайт\nзаказать сайт\nдоработать сайт\nпеределать сайт\nразработка сайта\nразработку сайта\nразработчика сайта\nвеб разработчика\nвеб-разработчика\nвеб студию\nвеб-студию\nлендинг\nлендинга\nлендингов\nlanding page\nwebsite developer\nweb developer\nwebsite development\nwebsite redesign\nbuild a website\nneed a website',
     COMMON_QUERIES + '\nЗаказы сайты\nТильда заказы',
     'Здравствуйте! Увидел ваш запрос по сайту. Я Разработчикислав, SignM, занимаюсь разработкой и доработкой сайтов. Задача ещё актуальна? Если да, расскажите, что нужно сделать и есть ли уже сайт — предложу подход и оценку.',
     'Hi! I saw your request about a website. I’m Vladislav from SignM; I build and improve websites. Is this still relevant? Tell me what you need and whether you have an existing site, and I can suggest an approach and estimate.'),
    ('ecommerce', 'Интернет-магазины и Битрикс',
     'интернет-магазин\nинтернет магазина\nинтернет-магазина\nинтернет магазин\nбитрикс\nbitrix\nаспрo\nаспро\naspro\nwoocommerce\nshopify\necommerce\ne-commerce\nonline store\nкаталог товаров\nвыгрузку товаров\nобмен остатков\nсинхронизацию остатков\nвыгрузка остатков\n1с интеграция',
     COMMON_QUERIES + '\nБитрикс чат\nИнтернет магазины чат',
     'Здравствуйте! Увидел вашу задачу по интернет-магазину. Занимаюсь магазинами на Битрикс, каталогами, обменом ценами и остатками. Подскажите, какая у вас платформа и что сейчас нужно настроить? Смогу оценить объём и сроки.',
     'Hi! I saw your ecommerce request. I work on Bitrix stores, product catalogs and price/stock integrations. Which platform do you use, and what needs to be changed? I can review the scope and timeline.'),
    ('bots', 'Telegram-боты',
     'телеграм бот\nтелеграм-бот\nтелеграм бота\nтелеграм-бота\ntelegram bot\ntelegram бота\ntelegram бот\ntelegram-бот\ntelegram-бота\nтг бота\nтг-бота\nчат-бот\nчат бота\nчат-бота\nбота для\nбот для\nразработчика бота\nchatbot\nchat bot',
     COMMON_QUERIES + '\nЗаказы боты\nРазработка ботов чат',
     'Здравствуйте! Увидел, что вам нужен бот. Я Разработчикислав, разрабатываю Telegram-ботов и интеграции. Что он должен делать: принимать заявки, записывать клиентов, отвечать на вопросы или связываться с CRM? Если задача актуальна, помогу определить объём.',
     'Hi! I saw your request for a bot. I build Telegram bots and integrations. What should it handle: enquiries, bookings, customer questions or CRM integration? If it’s still relevant, I can help define the scope.'),
    ('automation', 'Автоматизация и ИИ',
     'автоматизация\nавтоматизацию\nавтоматизации\nавтоматизировать\nintegration\nintegrations\nинтеграцию\nинтеграции\nn8n\nmake.com\nzapier\nнейросеть\nнейросети\nии-ассистента\nии ассистента\nии-бота\nии агента\nии-агента\nai agent\nвнедрить ии\nвнедрение ии\nвнедрить нейросеть\nавтоответы\nавтоответчик\nai assistant\nai automation\nworkflow automation\nbusiness automation',
     COMMON_QUERIES + '\nАвтоматизация бизнеса\nn8n чат',
     'Здравствуйте! Увидел ваш запрос по автоматизации. Занимаюсь интеграциями, обработкой заявок и ИИ-помощниками для бизнеса. Какой процесс сейчас отнимает больше всего времени и какие сервисы у вас уже используются? По этому смогу предложить решение.',
     'Hi! I saw your automation request. I work on integrations, enquiry processing and AI assistants for businesses. Which process takes the most time, and which tools do you currently use? That would help me suggest a solution.'),
    ('crm', 'CRM и обработка заявок',
     'crm\nсрм\nбитрикс24\nбитрикс 24\nbitrix24\namo\namocrm\nамо\nамосрм\nворонку продаж\nворонка продаж\nучет клиентов\nучёт клиентов\nобработку заявок\nобработка заявок\nlead management',
     COMMON_QUERIES + '\nБитрикс24 чат\namoCRM чат',
     'Здравствуйте! Увидел ваш запрос по CRM и заявкам. Я Разработчикислав, помогаю связать сайт, формы, ботов и CRM. Что нужно сейчас: внедрение с нуля или доработка работающей системы? Можем начать с короткого описания вашей задачи.',
     'Hi! I saw your CRM request. I help connect websites, forms, bots and CRM systems. Do you need a new setup or improvements to an existing system? A short description of your workflow would be a good starting point.'),
    ('marketing', 'Маркетинг и лидогенерация',
     'маркетолога\nмаркетолог\nмаркетинговое агентство\nмаркетинговую стратегию\nмаркетинговая стратегия\nпродвижение бизнеса\nпродвижением бизнеса\nпродвижению бизнеса\nлидогенерация\nлидогенерацию\nлидогенерации\nпривлечь клиентов\nпривлечение клиентов\ndigital marketing\nlead generation\nmarketing specialist\nmarketing agency',
     COMMON_QUERIES + '\nЗаказы маркетинг\nМаркетологи чат',
     'Здравствуйте! Увидел ваш запрос по продвижению. Я Разработчикислав, SignM. Могу разобрать связку рекламы, сайта и обработки заявок. Подскажите, что продвигаете, в каком регионе и что уже пробовали? После этого смогу предложить конкретные шаги.',
     'Hi! I saw your marketing request. I’m Vladislav from SignM. I can review how your advertising, website and enquiry handling work together. What do you promote, in which region, and what have you already tried?'),
    ('seo', 'SEO и аудит сайта',
     'seo\nсео\nсеошника\nсеошник\nseo-специалиста\nпродвижение сайта\nпродвижению сайта\nпродвинуть сайт\nаудит сайта\nаудита сайта\nускорить сайт\nскорость сайта\nиндексация\nиндексации\nwebsite audit\nsite audit\nsearch engine optimization\nwebsite speed',
     COMMON_QUERIES + '\nSEO чат\nSEO заказы',
     'Здравствуйте! Увидел ваш запрос по SEO или работе сайта. Занимаюсь техническим аудитом, индексацией и скоростью сайтов. Пришлите ссылку и опишите основную проблему — посмотрю, с чего стоит начать и как оценить работу.',
     'Hi! I saw your SEO or website performance request. I work on technical audits, indexing and site speed. Share the website and the main issue, and I can suggest where to start and how to scope the work.'),
]

TOPICS += [
    ('smm', 'SMM и ведение соцсетей',
     'smm\nсмм\nсммщика\nсммщик\nsmm-специалиста\nсмм-специалиста\nсмм специалиста\nведение соцсетей\nвести соцсети\nвести инстаграм\nведение инстаграма\nведение инстаграм\nпродвижение инстаграм\nпродвижение инстаграма\nпродвижения инстаграма\nпродвижение канала\nпродвинуть канал\nконтент-менеджера\nконтент менеджера\nконтент-план\nконтент план\nрилсмейкера\nрилсмейкер\nсоциальных сетей\nsocial media manager\nsocial media marketing\ninstagram promotion\ninstagram management\nsocial media management',
     COMMON_QUERIES + '\nSMM заказы\nSMM чат',
     'Здравствуйте! Увидел ваш запрос по соцсетям. Я Разработчикислав, SignM. Могу помочь с планом продвижения, контентом и автоматизацией обработки обращений. Какая у вас ниша, какие соцсети ведёте и какой результат нужен в первую очередь?',
     'Hi! I saw your social media request. I’m Vladislav from SignM. I can help plan promotion, content and enquiry automation. What is your business, which social platforms do you use, and what is your main goal?'),
    ('ads', 'Реклама и таргет',
     'таргетолог\nтаргетолога\nдиректолог\nдиректолога\nконтекстная реклама\nконтекстную рекламу\nконтекстной рекламы\nнастроить рекламу\nнастройку рекламы\nнастройке рекламы\nзапустить рекламу\nзапуск рекламы\nнастроить таргет\nрекламные кампании\nрекламных кампаний\nрекламу для бизнеса\ngoogle ads\nmeta ads\nfacebook ads\ninstagram ads\ntelegram ads\nyandex direct\nяндекс директ\nяндекс.директ\nавитолога\nавитолог\nпродвижение авито\nпродвижения авито\nпродвижение на авито\nppc specialist\npaid advertising\nmedia buyer',
     COMMON_QUERIES + '\nТаргет заказы\nДиректологи чат',
     'Здравствуйте! Увидел ваш запрос по рекламе. Я Разработчикислав, SignM. Подскажите, что продвигаете, в каком регионе и где планируете запуск? Если реклама уже работает, можем начать с разбора кампаний, посадочной страницы и обработки заявок.',
     'Hi! I saw your advertising request. I’m Vladislav from SignM. What do you promote, in which region, and on which platform? If campaigns are already running, we can start by reviewing the ads, landing page and enquiry handling.'),
]


def seed_marketing(store):
    if store.get('marketing_seed_v1'):
        apply_project_scope(store)
        return
    with store.db:
        for key,name,keywords,queries,*_ in TOPICS:
            data=dict(sources=[],keywords=words(keywords),required=words(INTENT),
                      excluded=words(EXCLUDED),substring=False,demand_only=True,
                      topic=key,discovery_queries=queries.splitlines())
            store.db.execute('INSERT INTO rules(name,data,enabled) VALUES(?,?,0)',
                             (name,json.dumps(data,ensure_ascii=False)))
    store.put('marketing_seed_v1',True)
    apply_project_scope(store)


def apply_project_scope(store):
    """One-time owner-only migration. Preserve edited keys, assignments and status."""
    if store.get('marketing_project_ru_v1'): return
    queries = {key: query.splitlines() for key, name, keys, query, *_ in TOPICS}
    with store.db:
        for rule in store.rules():
            data = {k:v for k,v in rule.items() if k not in ('id','name','enabled')}
            data.update(language='ru', contractor_only=True)
            data['required'] = words(INTENT)
            data['discovery_queries'] = queries.get(data.get('topic'), COMMON_QUERIES.splitlines())
            store.db.execute('UPDATE rules SET data=? WHERE id=?', (json.dumps(data,ensure_ascii=False),rule['id']))
    store.put('marketing_project_ru_v1',True)
