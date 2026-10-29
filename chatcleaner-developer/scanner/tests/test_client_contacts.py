"""Client leads remain candidates until the panel verifies a private author."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from client_contacts import annotate_client_contact


class ClientContactTests(unittest.TestCase):
    def setUp(self):
        self.lead = {'id': 7, 'sender_id': 123, 'author': '@synthetic_author',
                     'text': 'Ищу диджея на мероприятие', 'link': 'https://t.me/sample/9'}
        self.conversation = {'lead_id': 7, 'peer_id': 123, 'account_id': 555,
                             'last_sync': '2026-10-12T00:00:00+00:00'}

    def contact(self, lead=None, source=None, **kwargs):
        return annotate_client_contact(self.lead if lead is None else lead,
                                       source, **kwargs)['client_contact']

    def test_nonpersonal_or_missing_ids_have_no_confirmed_contact(self):
        for sender in (None, 0, -100123, True, '123', 2**63):
            with self.subTest(sender=sender):
                c = self.contact({**self.lead, 'sender_id': sender})
                self.assertEqual(c['status'], 'no_contact')
                self.assertFalse(c['author_verified'])
                self.assertFalse(c['ready'])

    def test_contacts_in_body_and_author_name_never_become_the_recipient(self):
        c = self.contact({**self.lead, 'sender_id': None,
                          'text': 'Заказ с биржи. Писать @synthetic_contact +7 000 000 00 00'})
        self.assertEqual(c['status'], 'no_contact')
        self.assertIn('body_contact_unverified', c['reason_codes'])
        self.assertNotIn('contact_username', c)
        self.assertNotIn('synthetic_contact', ' '.join(c['reasons']))

    def test_positive_sender_and_username_are_only_a_candidate(self):
        c = self.contact()
        self.assertEqual(c['status'], 'needs_review')
        self.assertFalse(c['author_verified'])
        self.assertFalse(c['customer_verified'])
        self.assertFalse(c['ready'])

    def test_personal_author_without_username_is_not_rejected(self):
        self.assertEqual(self.contact({**self.lead, 'author': 'Автор'})['status'], 'needs_review')

    def test_untrusted_lead_flags_cannot_promote_a_candidate(self):
        c = self.contact({**self.lead, 'author_verified': True, 'customer_verified': True,
                          'contact_status': 'confirmed_author', 'ready': True})
        self.assertEqual(c['status'], 'needs_review')
        self.assertFalse(c['ready'])

    def test_conversation_must_match_peer_account_and_lead_and_be_synced(self):
        variants = [{'peer_id': 124}, {'account_id': 556}, {'lead_id': 8},
                    {'last_sync': None}, {'last_sync': ''}, {'peer_id': '123'}]
        for change in variants:
            with self.subTest(change=change):
                c = self.contact(conversation={**self.conversation, **change}, account_id=555)
                self.assertEqual(c['status'], 'needs_review')
                self.assertFalse(c['author_verified'])
        self.assertFalse(self.contact(conversation=self.conversation)['author_verified'])

    def test_matching_conversation_confirms_author_but_not_customer(self):
        c = self.contact(conversation=self.conversation, account_id=555)
        self.assertEqual(c['status'], 'confirmed_author')
        self.assertTrue(c['author_verified'])
        self.assertFalse(c['customer_verified'])
        self.assertFalse(c['ready'])

    def test_linked_lead_may_use_an_existing_verified_conversation(self):
        conversation = {**self.conversation, 'lead_id': 6, 'linked_lead_ids': [7]}
        self.assertEqual(self.contact(conversation=conversation, account_id=555)['status'],
                         'confirmed_author')

    def test_self_bot_and_deleted_author_are_not_contactable_customers(self):
        cases = [({'sender_id': 555}, {'account_id': 555}),
                 ({'sender_is_bot': True}, {}), ({'sender_deleted': True}, {})]
        for change, kwargs in cases:
            with self.subTest(change=change):
                self.assertEqual(self.contact({**self.lead, **change}, **kwargs)['status'],
                                 'no_contact')

    def test_forwarded_metadata_requires_review_even_with_verified_author(self):
        for field, value in [('forwarded', True), ('fwd_from', {'from_id': 999}),
                             ('forward_origin', {'type': 'channel'}),
                             ('forward_from_chat', {'id': -1001})]:
            with self.subTest(field=field):
                c = self.contact({**self.lead, field: value},
                                 conversation=self.conversation, account_id=555)
                self.assertEqual(c['status'], 'needs_review')
                self.assertTrue(c['author_verified'])
                self.assertIn('forwarded_message', c['reason_codes'])
                self.assertFalse(c['ready'])

    def test_explicit_text_repost_marker_is_a_hint_and_never_confirmation(self):
        for text in ('Репост: ищу диджея', 'Переслано из канала\nИщу диджея',
                     'Forwarded from Example\nLooking for a DJ'):
            with self.subTest(text=text):
                c = self.contact({**self.lead, 'text': text})
                self.assertIn('repost_text_hint', c['reason_codes'])
                self.assertEqual(c['status'], 'needs_review')

    def test_source_aggregator_metadata_or_explicit_title_requires_review(self):
        for source, code in [({'is_aggregator': True}, 'aggregator_source'),
                             ({'kind': 'marketplace'}, 'aggregator_source'),
                             ({'title': 'Биржа заказов Example'}, 'aggregator_title_hint')]:
            with self.subTest(source=source):
                c = self.contact(source=source, conversation=self.conversation, account_id=555)
                self.assertEqual(c['status'], 'needs_review')
                self.assertTrue(c['author_verified'])
                self.assertIn(code, c['reason_codes'])

    def test_normal_source_title_is_not_automatically_an_aggregator(self):
        for title in ('Dubai Events', 'Заказы на DJ', 'Маркетинг', 'Работа и услуги'):
            with self.subTest(title=title):
                c = self.contact(source={'title': title}, conversation=self.conversation,
                                 account_id=555)
                self.assertEqual(c['status'], 'confirmed_author')

    def test_marketplace_links_are_review_hints_not_customer_contact(self):
        for url in ('https://kwork.ru/projects/1', 'https://www.fl.ru/projects/1',
                    'https://freelance.ru/projects/1', 'https://www.upwork.com/jobs/1',
                    'freelancer.com/projects/example'):
            with self.subTest(url=url):
                c = self.contact({**self.lead, 'text': 'Запрос: '+url},
                                 conversation=self.conversation, account_id=555)
                self.assertEqual(c['status'], 'needs_review')
                self.assertIn('marketplace_link', c['reason_codes'])
                self.assertFalse(c['customer_verified'])
        c = self.contact({**self.lead, 'text': 'https://kwork.ru.evil.example/request'})
        self.assertNotIn('marketplace_link', c['reason_codes'])

    def test_false_forwarded_metadata_is_not_a_repost_hint(self):
        c = self.contact({**self.lead, 'forwarded': False, 'fwd_from': None},
                         conversation=self.conversation, account_id=555)
        self.assertEqual(c['status'], 'confirmed_author')

    def test_annotation_preserves_lead_and_does_not_mutate_any_inputs(self):
        lead = {**self.lead, 'matches': [{'id': 1}], 'status': 'new'}
        source = {'title': 'Events'}
        conversation = deepcopy(self.conversation)
        snapshots = deepcopy((lead, source, conversation))
        result = annotate_client_contact(lead, source, conversation=conversation, account_id=555)
        self.assertEqual((lead, source, conversation), snapshots)
        self.assertIsNot(result, lead)
        for field, value in lead.items():
            self.assertEqual(result[field], value)
        self.assertEqual(result['status'], 'new')


if __name__ == '__main__':
    unittest.main()
