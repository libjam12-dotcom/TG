import json
import re
from antispam.domain.models import Action, Decision, MessageContext, Signal
from antispam.moderation.normalize import normalize, content_hash, urls, domain, domain_matches, word_spans, covered_by_phrases, canonicalize_keywords, fuzzy_keywords
from antispam.storage.repositories import Store
from antispam.moderation.advertising import ad_signals,is_request


class Engine:
    def __init__(self, store: Store, policy: dict):
        self.store, self.policy = store, policy

    def evaluate(self, ctx: MessageContext) -> Decision:
        chat=self.store.chat(ctx.chat_id) or {}
        version=chat.get('policy_version',1)
        if ctx.protected:
            return Decision(Action.ALLOW,(Signal('Защищённый администратор',0),),version)
        if self.store.user_policy(ctx.chat_id,ctx.user_id,'blocked'):
            action=Action.REVIEW if ctx.user_id is None else Action.BAN
            return Decision(action,(Signal('Ручной список блокировки',100),),version,action==Action.REVIEW)
        trusted=self.store.user_policy(ctx.chat_id,ctx.user_id,'trusted')
        text=normalize(ctx.text)
        all_urls=urls(ctx.text,ctx.links)
        hosts=[domain(u) for u in all_urls]
        exceptions=self.store.exceptions(ctx.chat_id)
        ad_config=self.store.setting(f'advertising:{ctx.chat_id}',self.store.setting('advertising:0',{}))
        ad_config={**ad_config,'enabled':self.store.setting(f'ad_enabled:{ctx.chat_id}',self.store.setting('ad_enabled:0',ad_config.get('enabled',False)))}
        ad_exempt=any(ex['kind']=='phrase' and 'advertising' in json.loads(ex['scope_json']).get('categories',[])
                      and normalize(ex['value'])==text for ex in exceptions)
        advertising=[] if trusted or ad_exempt else ad_signals(text,ad_config)
        request_only=is_request(text)
        ad_rule_ids=set(self.store.setting('ad_rule_ids:0',[]))
        matched_ad_rule=False
        non_ad_hard=False
        quarantine_deferred=False
        signals=[]
        actions=[]
        matched_kinds=[]
        obfuscated=set(self.store.setting('obfuscated_words:0',[])) | set(self.store.setting('learned_words:0',[]))
        rules=self.store.rules(ctx.chat_id)
        span_cache={r['id']:word_spans(text,r['pattern'],normalize(r['pattern']) in obfuscated) for r in rules if r['kind']=='word'}
        replacements=[(a,b,normalize(r['pattern'])) for r in rules if r['kind']=='word' and normalize(r['pattern']) in obfuscated for a,b in span_cache[r['id']]]
        # Ambiguous slang contributes only to combined offer evidence, never a standalone ban.
        for group in self.store.setting('contextual_slang:0',[]):
            replacements.extend((a,b,group['canonical']) for alias in group['aliases'] for a,b in word_spans(text,alias,True))
        offer_text=canonicalize_keywords(text,replacements)
        matched_offers=[offer for offer in self.store.setting('auto_ban_patterns:0',[])
                        if all(re.search(pattern,offer_text) for pattern in offer['all'])]
        learned_critical=set(self.store.setting('learned_critical_rules:0',[]))
        taught_match=any(r['id'] in learned_critical and (bool(span_cache[r['id']]) if r['kind']=='word' else normalize(r['pattern']) in text)
                         for r in rules)
        if taught_match and re.search(r'(?<!\w)(?:продам|продаю|доставка|заказ|за деньги|for sale|delivery|book now|per hour)(?!\w)',offer_text):
            matched_offers.append({'name':'Явное предложение по подтверждённому критическому правилу'})
        for rule in rules:
            if ad_exempt and rule['id'] in ad_rule_ids:continue
            if (ad_exempt or request_only) and rule['kind']=='phrase' and rule['action']=='review' and normalize(rule['pattern']) in ad_config.get('ad_review_phrases',[]):continue
            kind=rule['kind']; pattern=rule['pattern']
            spans=span_cache[rule['id']] if kind=='word' else []
            match=(kind=='word' and bool(spans) or kind=='phrase' and normalize(pattern) in text
                   or kind=='domain' and any(domain_matches(h,pattern) for h in hosts)
                   or kind=='url' and any(normalize(u)==normalize(pattern) for u in all_urls))
            if not match or (trusted and kind in ('word','phrase')): continue
            excluded=False
            covered_phrases=[]
            for ex in exceptions:
                scope=json.loads(ex['scope_json'])
                applies=ex['rule_id']==rule['id'] or kind in scope.get('categories',[])
                if not applies: continue
                if ex['kind']=='phrase' and normalize(ex['value']) in text:
                    if scope.get('match_mode')=='whole_message':excluded=excluded or normalize(ex['value'])==text
                    elif scope.get('match_mode')=='covered_occurrences' and kind=='word':covered_phrases.append(ex['value'])
                    else:excluded=True
                if ex['kind']=='user' and str(ctx.user_id)==ex['value']: excluded=True
                if ex['kind']=='domain' and any(domain_matches(h,ex['value']) for h in hosts):
                    # Must not exempt a forbidden domain because an unrelated allowed URL is also present.
                    if kind=='domain' and all(not domain_matches(h,pattern) or domain_matches(h,ex['value']) for h in hosts): excluded=True
            if excluded or (not matched_offers and covered_by_phrases(text,spans,covered_phrases)): continue
            signals.append(Signal(f"Правило #{rule['id']}: {kind} / {pattern}",100,rule['id']))
            matched_ad_rule=matched_ad_rule or rule['id'] in ad_rule_ids
            matched_kinds.append(kind)
            action=Action(rule['action'])
            if rule['id'] in ad_rule_ids and not ad_config.get('enabled'):action=Action.REVIEW
            if action==Action.BAN and not self.store.setting(f'autoban:{ctx.chat_id}',False):
                action=Action.DELETE
            # Preserve the matched non-ad action before quarantine can defer a
            # restriction. Otherwise a coincident ad rule can mislabel the
            # event as an advertisement and send an unrelated ad notice.
            non_ad_hard=non_ad_hard or action in (Action.DELETE,Action.BAN,Action.RESTRICT) and rule['id'] not in ad_rule_ids
            if action==Action.RESTRICT:
                quarantine_enabled=self.store.setting(f'quarantine_auto:{ctx.chat_id}',False)
                if not quarantine_enabled or ctx.user_id is None:
                    quarantine_deferred=True
                    action=Action.REVIEW
            if ctx.user_id is None and action==Action.BAN: action=Action.DELETE
            actions.append(action)
        if not trusted and matched_offers and not actions:
            signals.append(Signal('Сочетание признаков: '+matched_offers[0]['name'],100))
            actions.append(Action.DELETE);matched_kinds.append('phrase')
        if actions:
            rank={Action.REVIEW:1,Action.DELETE:2,Action.RESTRICT:3,Action.BAN:4}
            action=max(actions,key=lambda a:rank[a])
            reporting=self.store.setting('reporting_context:0',[])
            is_reporting=any(normalize(p) in text for p in reporting)
            if action in (Action.DELETE,Action.BAN,Action.RESTRICT) and all(k in ('word','phrase') for k in matched_kinds) and is_reporting:
                signals.append(Signal('Возможное предупреждение или новость: проверить контекст',0))
                action=Action.REVIEW
            if quarantine_deferred and action==Action.REVIEW:
                signals.append(Signal('Карантин отключён или автор не определён: ручной разбор',20))
                return Decision(Action.REVIEW,tuple(signals),version,True)
            if action in (Action.DELETE,Action.REVIEW) and not is_reporting:
                if matched_offers:
                    offer=matched_offers[0]
                    can_ban=self.store.setting(f'autoban:{ctx.chat_id}',False) and (ctx.user_id is not None or ctx.is_manual_check)
                    action=Action.BAN if can_ban else Action.DELETE
                    signals.append(Signal(offer['name']+(': удалить сообщение и заблокировать автора' if can_ban else ': удалить сообщение'),100))
            if ((not matched_offers or is_reporting) and action==Action.REVIEW and advertising
                    or not matched_offers and action==Action.DELETE and matched_ad_rule and not non_ad_hard):
                signals.append(Signal('Несогласованная реклама: '+(advertising[0] if advertising else 'подтверждённый шаблон'),100))
                return Decision(Action.DELETE,tuple(signals),version,False,'advertisement')
            return Decision(action,tuple(signals),version,action in (Action.REVIEW,Action.RESTRICT))
        if trusted:
            return Decision(Action.ALLOW,(Signal('Доверенный пользователь: исключение по словам и поведению',0),),version)
        if advertising:
            return Decision(Action.DELETE,(Signal('Несогласованная реклама: '+advertising[0],100),),version,False,'advertisement')
        uncertain=fuzzy_keywords(text,self.store.setting('fuzzy_review_targets:0',[]))
        uncertain += [(m.group(0),'маскированное чувствительное слово') for pattern in self.store.setting('review_spelling_patterns:0',[]) for m in re.finditer(pattern,text) if normalize(m.group(0)) not in obfuscated]
        if uncertain:
            return Decision(Action.REVIEW,tuple(Signal(f'Похожее написание: {token} / {reference}; нужна ручная проверка',20) for token,reference in uncertain[:5]),version,True)
        behavior_exempt=any(ex['kind']=='phrase' and (normalize(ex['value'])==text if json.loads(ex['scope_json']).get('match_mode')=='whole_message' else normalize(ex['value']) in text) and 'behavior' in json.loads(ex['scope_json']).get('categories',[]) for ex in exceptions)
        if not ctx.is_manual_check and not behavior_exempt:
            behavior=self.store.setting(f'behavior:{ctx.chat_id}',self.store.setting('behavior:0',self.policy['behavior']))
            since=ctx.observed_at-max(behavior['rate_window_seconds'],behavior['duplicate_window_seconds'],behavior['cross_chat_window_seconds'])
            recent=self.store.recent(ctx,since)
            same_chat=[m for m in recent if m['chat_id']==ctx.chat_id]
            rate=sum(m['observed_at']>=ctx.observed_at-behavior['rate_window_seconds'] for m in same_chat)+1
            digest=content_hash(ctx.text+'\n'+'\n'.join(ctx.links))
            duplicates=[m for m in same_chat if m['content_hash']==digest and m['observed_at']>=ctx.observed_at-behavior['duplicate_window_seconds']]
            distinct={m['chat_id'] for m in recent if m['content_hash']==digest and m['observed_at']>=ctx.observed_at-behavior['cross_chat_window_seconds']} | {ctx.chat_id}
            if rate>=behavior['review_at_messages']:signals.append(Signal('Частые сообщения',30))
            if text and len(duplicates)+1>=behavior['review_at_duplicates']:signals.append(Signal('Повторяющийся текст',40))
            if text and len(distinct)>=behavior['review_at_distinct_chats']:signals.append(Signal('Одинаковая рассылка по группам',40))
            newcomer=self.policy['newcomer']
            is_new=(ctx.joined_at is not None and 0<=ctx.observed_at-ctx.joined_at<=newcomer['window_seconds'] and ctx.first_messages_seen<newcomer['first_messages'])
            if is_new and all_urls:signals.append(Signal('Ссылка в первых сообщениях после вступления',10))
        if signals:
            # Opt-in combined evidence may remove a message; a lone link/new arrival never qualifies.
            if self.store.setting(f'behavior_auto:{ctx.chat_id}',False) and sum(s.weight for s in signals)>=70:
                return Decision(Action.DELETE,tuple(signals),version)
            return Decision(Action.REVIEW,tuple(signals),version,True)
        return Decision(Action.ALLOW,(),version)
