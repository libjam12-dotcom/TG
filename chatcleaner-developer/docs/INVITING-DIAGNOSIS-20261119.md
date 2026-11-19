# Проверка инвайтинга — 19.11.2026

Область: клиентский Chatcleaner. Выполнено только чтение переданных квитанций, исходников и официальной документации; создано два документа в выделенном временном каталоге. Код, аккаунты, сервер и очереди этим обзором не менялись. Секреты и реальные клиентские идентификаторы в документы не включены.

## Доказательства и пределы

Источник A: переданная квитанция `invite-192.json`. Источник B: `invite-193.json`. Они не пересоздавались. Идентификаторы заданий служат только локальными именами источников; публичный промпт использует A/B.

Прочитаны `Technical-dossier.md` и `Developer-spec.md` от 15.11, локальные `scanner/manual_invite_campaign.py`, `operations.py`, `invite_progress.py`, frontend и более свежий предоставленный координатором снимок `panel-live-source-20261119`. Заявленная координатором метка серверного релиза: `direct-invite-history-pr1-20261117-r1`. Получение с сервера не повторялось. Ни работа текущего процесса после снимка, ни соответствие любому публичному Git HEAD этим обзором не доказаны.

SHA-256 прочитанных серверных файлов:

| Файл | SHA-256 |
|---|---|
| scanner/manual_invite_campaign.py | c3bdc3f5546105919bd15b6c787f70f127b6ee1430fe12b0ee1287c7584243e6 |
| scanner/operations.py | d7db9b4c53e5f3ce9ae5ce16ffa4bf6cb6afbe675767cc0774ca8f41f5e53bc0 |
| scanner/invite_progress.py | 2701575d2d64885a01b0573a6a5070ed899192c0b9d18f523e157d497dda2aad |
| web/manual-invite-campaign.js | 8c6ffc3c5fbe0838bf7e273184b4be427fc0853024c6d6d54cd20da85509a700 |

## A: защита истории сработала

`state=completed`, `attempts_claimed=0`, counts attempts/added=0, duplicate_skipped=1, diagnosis `target_history_exists`, `attempted_at=null`, `invitation_requested=false`. Live manual `historical` (258–263) проверяет предыдущие invite-строки с attempted_at/значимыми состояниями и op_invited; `step` (822–825) возвращает дубль до read_membership и claim. Отсюда не следует, что человек сейчас состоит: актуальное членство в этой ветке не проверялось. Это не новая ошибка Telegram и не основание очищать дедупликацию.

## B: известный ответ, неизвестный результат добавления

Один claim, `state=stopped`, `stop_reason=invite_membership_unconfirmed`; один uncertain, added=0. Сохранён ответ `InvitedUsers`, `missing_invitees_present=true`, missing_invitees_count=0, `updates_type=Updates`, `update_types=[]`, `add_user_events=[]`. До вызова и три раза после него получен `UserNotParticipantError`. Проверки после: 14:08:09.501736Z, 14:08:10.561558Z, 14:08:12.623655Z. При проверках участник не подтверждён, окно ограничено несколькими секундами.

В live `step` (835–854) evidence создаётся только после возврата InviteToChannelRequest; следовательно это не отсутствие вызова или timeout. `request_outcome_unknown=true` в существующем контракте отражает неопределённость бизнес-эффекта, хотя ответ уже известен. Корректное расширение: `dispatch_status=responded`, `outcome=uncertain`, `membership_after=false` с timestamp, а не `dispatch_status=unknown` только из-за отсутствия членства.

Не установлены конкретная серверная причина, privacy/mutual/Premium, PEER_FLOOD, лимит, бан, позднее членство или кратковременное добавление/выход. В квитанции нет соответствующего RPC exception/missing флага/служебного события. Нельзя ранжировать их как доказанные вероятности. Наиболее узкий вывод: нет наблюдаемого подтверждённого эффекта direct invite в окне проверки. Fail-closed stop и запрет повторов соответствуют имеющемуся evidence; ослаблять их ради счётчика нельзя.

Возможные направления проверки, а не найденные причины:

- Серверный no-op/фильтрация без объясняющего payload: согласуется с пустым ответом и false, но документированного exact enum нет.
- Задержка видимости либо вступление с быстрым удалением: короткое окно этого не исключает, но положительного события нет.
- Несоответствие deployed SDK/API layer или корреляции account/target/user: проверить manifest и выбранные закреплённые peer без повторного write. В прочитанном коде binding защищён guards; его поломка этим обзором не обнаружена.

Следующий диагностический шаг — отдельное согласованное read-only reconciliation тех же UID/target и доступных административных событий. Позднее membership=true подтверждает состояние на момент чтения, но не причинность прошлой попытки. Не увеличивать бесконечно polling и не повторять invite для проверки гипотезы.

## Подтверждённые текущим снимком дефекты

Оценки взяты из технического досье как экспертные частота×ущерб; ниже все произведения ≥5. Это наблюдения по исходнику, не воспроизведённые live-инциденты каждого сценария.

| Дефект | Доказательство | Частота×ущерб | Действие |
|---|---|---:|---|
| Сводка manual active отображается через paused SQL parent | live operations.py:686–696 + invite_progress.py:108–126; manual job намеренно paused | 8×6=48 | Добавить manual controller state в read model, сохранить запрет generic worker |
| Сохранённая RPC metadata не отображается | web/manual-invite-campaign.js:64–70 читает code/reason; operations.py:215–248 хранит rpc | 7×6=42 | Безопасно показать enum/code/constructor/phase; null честно |
| claim приписывает invocation до SDK | manual:765–784 ставит invitation_requested=true; SDK только :835; public :539–553 делает bool(attempts) | 2×8=16 | Развести claim/call_entered/responded/unknown и бизнес-исход; legacy не переинтерпретировать |
| can_resume пропускает gates | manual:539–553 против enforcement context/guard/start | 6×5=30 | Единая серверная eligibility, точная причина и время |
| Day cap имеет неоднозначный scope | manual:249–251 считает все op_items данного sender UID; редактируемый порог campaign config | 6×7=42 | Утвердить account policy versus local threshold; никакого скрытого увеличения общего предохранителя |
| Goal precedence | manual:900–906 ставит max/day cap перед success_goal | 4×4=16 | При достижении цели терминальное завершение с сохранением прочих причин |

Исторический release mismatch доказан досье на 15.11; текущий fresh server snapshot новее локального. Его наличие не закрывает воспроизводимость Git/dist автоматически и не разрешает откат на архив. Нужен manifest принятого release, но обзор не объявляет новую live-ошибку развёртывания.

Открытые продуктовые ограничения: raw username quick до обработки не имеет старого UID; terminal Stop/файловый импорт/approval заявок отсутствовали в описанном ручном контуре; парсинг аудитории не гарантирует полный список или согласие. Влияние многократного guard на большой истории — непроверенная гипотеза производительности. Эти вопросы не превращены в расширение текущего задания.

## Первичные источники

- [channels.inviteToChannel](https://core.telegram.org/method/channels.inviteToChannel): direct invite для канала/супергруппы, возвращает messages.InvitedUsers; отдельные документированные ошибки включают права, mutual contact и privacy. Перечень допустимых ошибок метода не доказывает, какая произошла в B.
- [messages.invitedUsers](https://core.telegram.org/constructor/messages.invitedUsers): updates и список тех, кого не удалось пригласить. Пустой missing список не заменяет read-back членства; корневую причину B страница не объясняет.
- [missingInvitee](https://core.telegram.org/constructor/missingInvitee): флаг premium_would_allow_invite относится к условию direct invite; premium_required_for_pm — к отправке ссылки личным сообщением при privacy restriction. В B записей/флагов нет, поэтому эти диагнозы не применены.
- [channels.getParticipant](https://core.telegram.org/method/channels.getParticipant): чтение участника; USER_NOT_PARTICIPANT отличается от INVALID/PRIVATE/ADMIN ошибок. В проекте `read_membership` (:832 и далее) переводит именно UserNotParticipantError в member=false.

Документация проверена браузером 19.11.2026. Её текущий layer не подменяет установленную версию SDK. Живые действия и новые Telegram-чтения в этом обзоре не выполнялись. Поведенческие regression tests для новых исправлений остаются обязанностью исполнителя; этот обзор не заявляет их прохождение.
