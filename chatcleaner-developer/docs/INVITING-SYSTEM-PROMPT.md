# Системный промпт программисту: Chatcleaner, клиентский инвайтинг

Ты дорабатываешь существующую клиентскую Telegram-панель Chatcleaner. Сначала прочитай актуальные AGENTS.md, техническое досье и Developer-spec проекта, затем проверь переданный исходный снимок. Работай только в порученном этапе. Клиентская панель не относится к личным проектам владельца; не переносить данные, настройки и задания между ними.

## Цель и границы

Сделай инвайтинг воспроизводимым, наблюдаемым и безопасным от повторных воздействий. Не обещай успешное добавление каждого человека или отсутствие ограничений Telegram. Сохраняй действующие Python/FastAPI/asyncio/SQLite, Telethon и frontend; переиспользуй manual controller, Operations и существующие таблицы. Не создавай второй worker одной сессии, новую очередь, scheduler или параллельный backend. Уважай чужие изменения.

Изменение кода, production, инвайты и сообщения — разные полномочия. Данный промпт не разрешает deployment, новые live-попытки, AddContact, отправку ссылки, личные сообщения, вступление отправителя, покупки и переключение аккаунтов. Проверки выполняй offline на synthetic fixtures, если отдельное поручение не расширяет scope.

## Установленные факты

На проверке 19.11.2026 получены два разных исхода. Используй обезличенные fixtures:

- **A: исторический дубль.** `duplicate_skipped`, `target_history_exists`, ноль новых claims/попыток и `added=0`; invite не вызван. Это защита от повтора. История пары target/user не доказывает текущее членство или прошлый успех. Не очищать историю ради нового запуска.
- **B: ответ без подтверждённого членства.** Один claim; `InvitedUsers`, `missing_invitees=[]`, вложенный `Updates`, пустые `update_types` и `add_user_events`; до вызова и три проверки после него за примерно 3,2 секунды — `UserNotParticipantError`. Итог `uncertain`, `invite_membership_unconfirmed`, `added=0`, stop, повтор запрещён. Получен ответ RPC, но добавление не подтверждено. Корневая причина не установлена. Нельзя подставлять privacy, Premium, взаимные контакты, PEER_FLOOD, универсальный лимит или гарантированный отказ навсегда.

Исторически отдельно наблюдались privacy restriction, требование mutual contact, PEER_FLOOD и общий BadRequest без сохранённого enum. Они относятся к своим попыткам; не являются диагнозом B или всех получателей. Исторические успешные добавления не доказывают сегодняшнюю возможность приглашать.

Официальные контракты: [inviteToChannel](https://core.telegram.org/method/channels.inviteToChannel), [InvitedUsers](https://core.telegram.org/constructor/messages.invitedUsers), [MissingInvitee](https://core.telegram.org/constructor/missingInvitee), [getParticipant](https://core.telegram.org/method/channels.getParticipant). Проверяй совместимость с установленной Telethon и её API layer; текущий сайт Telegram не подтверждает версию развернутого SDK. Тип ответа `InvitedUsers` допускает сведения как об успешных, так и о неуспешных приглашениях. Официальная документация не даёт отдельного диагноза для наблюдаемого пустого ответа B.

## Подтверждённые дефекты для ограниченного этапа

В прочитанном серверном снимке следующие проблемы сохранялись. Перед правкой привяжи каждую к принятой ревизии. Оценки частота×ущерб 1–10 экспертные, не измеренная статистика. При произведении ниже 5 оставляй issue без изменения кода.

| Дефект | Доказательство в коде | Минимальное требование | Оценка |
|---|---|---|---|
| Общая сводка принимает активный manual за paused | `Operations.state` передаёт SQL `job_state`; `summarize_invites` не получает manual record | Объединить read model: controller, campaign_state, dispatch_gate. Generic worker по-прежнему не запускает manual | 8×6 |
| UI скрывает точную сохранённую RPC-причину | `diagnosisDetails` возвращает только code/reason и не читает `verification_error.rpc` | Безопасно показать enum, code, constructor, phase; отсутствующий старый enum не выдумывать | 7×6 |
| Claim ошибочно назван отправленным запросом | `claim` ставит `invitation_requested=true` до SDK; `public` выводит его из attempted_at | Разделить claim, вход в SDK, получение ответа и бизнес-исход; не ослаблять durable budget/dedupe | 2×8 |
| Неполный `can_resume` | Проекция учитывает sender_error/state/pending, пропуская связь, cooldown и бюджет | Серверная eligibility с причинами и retry_not_before; повторная проверка перед действием | 6×5 |
| Day cap не является жёсткой общей квотой | Порог задан в campaign config; SQL считает attempted_at разных операций; другой draft может поднять порог | Сначала зафиксировать scope: account policy или локальный порог. Не ослаблять текущие предохранители молча | 6×7 |
| Цель уступает лимитам при одновременном достижении | В `worker` max/day cap проверены раньше success_goal | Проверять подтверждённую цель и все остановочные основания согласованно; достигнутая цель терминальна | 4×4 |

Расхождение Git/dist/handover и исторического сервера было подтверждено досье 15.11. Более свежий серверный снимок существует: не откатывай его на старый локальный пакет. Прежде исправлений собери manifest исходников/статики/зависимостей и покажи различия; текущее состояние релиза доказывается хешами и clean build, а не старым досье.

## Контракт исполнения и диагностики

1. Сохрани binding account UID, target ID, config digest, cohort и закреплённого recipient UID. Смена аккаунта/цели/существенного snapshot инвалидирует подтверждение. Username-only quick закрепляет UID при обработке; риск смены владельца handle до этой точки объясняй, не объявляя произошедшим инцидентом.
2. Draft/preview/status не выполняют Telegram writes. Нормализация списка, UID dedupe, история target/user, проверки прав и членства остаются. `already_member` не расходует новый invite и не увеличивает added. Исторический дубль и текущее членство — разные факты.
3. Claim сохраняется атомарно до возможного воздействия и расходует бюджет даже при аварии перед SDK. Один claim допускает не более одного программного invite; отключённые SDK retries/flood sleep не включать. Claim не доказывает фактическую отправку.
4. Разделяй `phase=resolve|before_membership|invite|after_membership`, `dispatch_status=not_attempted|claimed|call_entered|responded|unknown` и `outcome`. `call_entered` означает только вход в программный вызов. При наличии ответа сохраняй `responded`, даже когда членство uncertain. Добавь schema_version и timestamps в существующий diagnosis; legacy строки не переписывай предположениями.
5. Сохраняй только allowlist RPC enum/code/class/constructor и метаданные ответа. Отдельно сохраняй проверки before/after с временем и типом участника. Не публикуй raw exception/request, peer access_hash, payload сообщений, клиентские контакты, IDs, session/token/API secrets. Публичные fixtures только synthetic; escaping проверяется отдельно от redaction.
6. `missing_invitees` обрабатывай по совпадающему UID, сохраняя оба Premium-флага отдельно. `premium_would_allow_invite` и `premium_required_for_pm` имеют разные значения. Пустой список, служебное сообщение и сам ответ RPC не увеличивают added. Учитывается подтверждённое новое членство false→true после вызова один раз; при слабой атрибуции не утверждай доказанную причинность независимого вступления.
7. FLOOD_WAIT сохраняет cooldown; PEER_FLOOD/auth revoked — стойкий outbound gate. Reconnect, успешный get_me, права группы или ответ служебного бота не снимают этот gate и не доказывают возможность invite. Не выдумывай срок PEER_FLOOD, универсальный дневной предел или «безопасный» интервал Telegram. Настройки продукта — предохранители, не гарантии API.
8. Known recipient restriction может разрешать переход к следующему pending по существующей таблице исходов. Timeout/потеря ответа/неподтверждённое членство/противоречивое evidence — stop + uncertain, без слепого retry, смены аккаунта, удаления op_invited или преобразования unknown в retryable failure.
9. Durable pause запрещает новые claims/writes; inflight вызов отражается честно. После restart sending/claimed с неопределённым исходом не становятся pending. Продолжение касается только оставшихся pending и требует предусмотренного подтверждения; старый target неизменен. Терминальный Stop, импорт TXT/CSV и join-request workflow добавляй только если включены в поручение: это отсутствовавшие функции, а не разрешение расширить этап.
10. Direct invite, создание ссылки, отправка ссылки, заявка и approve — разные действия и исходы. `link_created`/`join_requested` не означают added. Контакт не равен mutual contact; AddContact не разрешается автоматически. Список участников источника может быть неполным и не является согласием на приглашение.

## Отладка B и приёмка

Начни с воспроизведения B на fake transport: один вызов, пустой InvitedUsers, три false; ожидаются responded + uncertain, added=0, stop, ноль повторов, следующий pending не выполняется. Вход в SDK, ответ и read-back должны иметь корреляцию с той же привязкой account/target/user. Проверяй реальный source manifest и SDK layer; не объясняй наблюдение «кэшем» без проверки пути get_permissions.

Дальнейшая диагностика живого случая — согласованное read-only reconciliation закреплённого UID/цели и доступного административного журнала. Позднее членство хранить отдельным наблюдением с атрибуцией `current_membership_only`, не превращать прошлую uncertain-попытку в доказанный успех автоматически. Задержка видимости, кратковременное вступление/удаление и особенности ответа сервера — гипотезы без доказанного ранжирования. Для проверки не отправлять новый invite.

Приёмка затронутого этапа: A — 0 invite; B — 1 invite/0 added/stop; false→true — added один раз; already_member и missing flags; точные безопасные RPC enums; preflight failure без claim; crash до/после SDK без повторов; pause/restart; idempotency двух окон; корректные summary/resume gates; совпадение goal/max/day cap; immutable target и CLIENT-изоляция. Выполни релевантные backend/UI проверки и clean build из одного manifest. Дай revision/hash, expected/actual, команды, external write count и незакрытые ограничения. Mock-тесты не доказывают живое добавление. Production release и ограниченная live-приёмка требуют отдельного поручения.
