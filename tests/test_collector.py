"""采集端行为检查：以合成来源的实际请求为依据。

覆盖：多语种变体共存、304 复用与正文缺失重取、Vary 扩展、别名改指向、
凭据 origin 范围、取消订阅互不影响、断点续传（强/弱验证器、If-Range
不符、416 恢复）、崩溃后 repair 核实，以及空件拒绝。
"""
import gzip
import http.client
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collector import Collector, CollectorError, EmptyDeliveryError
from collector import httpclient
from collector.store import Store
from collector.vary import cache_key, normalize_vary, request_fingerprint
from source.server import running_source

ZH = {'Accept-Language': 'zh', 'Accept-Encoding': 'gzip'}
EN = {'Accept-Language': 'en', 'Accept-Encoding': 'identity'}


def post_advance(base, payload):
    from urllib.parse import urlsplit
    parts = urlsplit(base)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=10)
    conn.request('POST', '/__admin/advance', json.dumps(payload),
                 {'Content-Type': 'application/json'})
    response = conn.getresponse()
    assert response.status == 200
    response.read()
    conn.close()


class CollectorCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='collector-test-')
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.store = Store(Path(self.tmp) / 'store')
        self.collector = Collector(self.store)

    def seed_partial(self, url, req_headers, wire, etag, vary, keep):
        """模拟一次中断的下载：只落盘前 keep 字节。"""
        key = cache_key(url, vary, req_headers)
        bin_path = self.store.partial_bin_path(key)
        bin_path.write_bytes(wire[:keep])
        self.store.partial_put(key, {
            'url': url,
            'requested_via': [url],
            'etag': etag,
            'weak': str(etag).startswith('W/'),
            'resumable': bool(etag) and not str(etag).startswith('W/'),
            'vary': list(vary),
            'request_fingerprint': request_fingerprint(req_headers),
            'total': len(wire),
            'have': keep,
        })
        return key

    # ---- 多语种与字节身份 ----

    def test_variants_coexist_with_distinct_identities(self):
        with running_source() as (base, source):
            zh = self.collector.subscribe('小李', base + '/document', ZH)
            en = self.collector.subscribe('小王', base + '/document', EN)
            d_zh = self.store.delivery_get(self.collector.claim(zh))
            d_en = self.store.delivery_get(self.collector.claim(en))
            self.assertEqual(d_zh['content_language'], 'zh')
            self.assertEqual(d_en['content_language'], 'en')
            body_zh = self.store.cas_read(d_zh['body_ref']['sha256'])
            body_en = self.store.cas_read(d_en['body_ref']['sha256'])
            self.assertEqual(body_zh.decode(), '合成披露正文')
            self.assertEqual(body_en.decode(), 'Synthetic disclosure')
            # 传输字节与解码正文分别登记，gzip 原件可回查
            wire_zh = self.store.cas_read(d_zh['wire_ref']['sha256'])
            self.assertEqual(gzip.decompress(wire_zh), body_zh)
            self.assertNotEqual(d_zh['wire_ref']['sha256'], d_en['wire_ref']['sha256'])
            self.assertEqual(len(self.store.cache_entries()), 2)
            # 交付件记录请求条件、时刻与转址链
            self.assertEqual(d_zh['request_headers']['accept-language'], 'zh')
            self.assertIn('fetched_at_iso', d_zh)
            self.assertEqual(d_zh['chain'], [])

    def test_revalidate_reuses_source_confirmation(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            self.collector.claim(sub)
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'revalidated')
            self.assertTrue(result['reused_source_confirmation'])
            self.assertEqual(result['bytes_downloaded'], 0)
            # 来源确实收到了带 If-None-Match 的条件请求
            conditional = [e for e in source.log
                           if e['headers'].get('if-none-match')]
            self.assertTrue(conditional)
            # 刷新记录里能辨认复用与重下
            report = self.collector.refresh_report()
            self.assertEqual(report[0]['outcome'], 'revalidated')

    def test_304_with_missing_body_refetches(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            self.collector.claim(sub)
            entry = self.store.cache_entries()[0]
            # 只剩缓存登记，文件丢失
            for ref in ('wire_ref', 'body_ref'):
                os.remove(self.store.cas_path_for(entry[ref]['sha256']))
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'downloaded')
            events = [e['event'] for e in self.store.journal_entries()]
            self.assertIn('revalidate_missing_body', events)
            restored = self.store.cache_get(entry['cache_key'])
            self.assertTrue(self.collector._entry_content_ok(restored))
            body = self.store.cas_read(restored['body_ref']['sha256'])
            self.assertEqual(gzip.decompress(
                self.store.cas_read(restored['wire_ref']['sha256'])), body)

    # ---- Vary 与别名 ----

    def test_vary_extension_revalidates_and_keeps_history(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            self.collector.claim(sub)
            old_key = self.store.cache_entries()[0]['cache_key']
            post_advance(base, {'phase': 2})
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'downloaded')
            entries = self.store.cache_entries()
            self.assertEqual(len(entries), 2)
            new_entry = self.store.cache_get(result['cache_key'])
            self.assertIn('x-disclosure-scope', new_entry['vary'])
            # 历史条目仍在，可回查，但键不同、不会误中
            self.assertIsNotNone(self.store.cache_get(old_key))
            self.assertNotEqual(old_key, result['cache_key'])

    def test_alias_change_does_not_rewrite_old_deliveries(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/alias', ZH)
            first = self.store.delivery_get(self.collector.claim(sub))
            self.assertTrue(first['final_url'].endswith('/document'))
            self.assertEqual(first['chain'][0]['status'], 301)
            post_advance(base, {'phase': 2})
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'downloaded')
            second = self.store.delivery_get(self.collector.claim(sub))
            self.assertTrue(second['final_url'].endswith('/other'))
            body2 = self.store.cas_read(second['body_ref']['sha256'])
            self.assertIn(b'other', body2)
            # 旧交付件的解释不变：仍指向 /document 时的内容
            again = self.store.delivery_get(first['delivery_id'])
            self.assertTrue(again['final_url'].endswith('/document'))
            body1 = self.store.cas_read(again['body_ref']['sha256'])
            self.assertNotIn(b'other', body1)

    def test_credentials_stay_within_authorized_origin(self):
        with running_source() as (base, source):
            collector = Collector(self.store, credentials={base: 'Bearer secret-token'})
            sub = collector.subscribe('小李', base + '/external', ZH)
            delivery = collector.store.delivery_get(collector.claim(sub))
            hop = delivery['chain'][0]
            self.assertEqual(hop['status'], 302)
            self.assertIn('localhost', hop['to'])
            self.assertEqual(hop['credentials'], 'dropped:authorization')
            # 源站请求带了凭据，公共站请求没有
            external = [e for e in source.log if e['path'] == '/external']
            self.assertEqual(external[0]['headers'].get('authorization'),
                             'Bearer secret-token')
            public = [e for e in source.log
                      if e['path'] == '/document'
                      and e['headers'].get('host', '').startswith('localhost')]
            self.assertTrue(public)
            self.assertNotIn('authorization', public[0]['headers'])
            # 交付件与刷新记录都不暴露凭据
            self.assertNotIn('secret', json.dumps(delivery, ensure_ascii=False))
            self.assertNotIn('secret', json.dumps(
                self.store.journal_entries(), ensure_ascii=False))

    def test_unsubscribe_keeps_shared_content_for_others(self):
        with running_source() as (base, source):
            direct = self.collector.subscribe('小李', base + '/document', ZH)
            alias = self.collector.subscribe('小王', base + '/alias', ZH)
            d1 = self.store.delivery_get(self.collector.claim(direct))
            d2 = self.store.delivery_get(self.collector.claim(alias))
            # 同一份内容共享存储
            self.assertEqual(d1['body_ref']['sha256'], d2['body_ref']['sha256'])
            self.assertTrue(self.collector.unsubscribe(direct))
            # 取消一人的订阅：其交付件留档，他人原件仍在
            self.assertIsNone(self.store.subscription_get(direct))
            self.assertIsNotNone(self.store.delivery_get(d1['delivery_id']))
            self.assertTrue(self.store.cas_verify(d2['body_ref']['sha256']))
            d3 = self.store.delivery_get(self.collector.claim(alias))
            self.assertEqual(d3['body_ref']['sha256'], d2['body_ref']['sha256'])

    # ---- 断点续传 ----

    def test_resume_completes_partial_download(self):
        with running_source() as (base, source):
            final_url, status, hdrs, wire, _ = httpclient.get(base + '/document', ZH)
            hdrs = {k.lower(): v for k, v in hdrs}
            keep = len(wire) // 2
            self.seed_partial(final_url, ZH, wire, hdrs['etag'],
                              normalize_vary(hdrs['vary']), keep)
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'resumed')
            self.assertEqual(result['bytes_downloaded'], len(wire) - keep)
            entry = self.store.cache_get(result['cache_key'])
            body = self.store.cas_read(entry['body_ref']['sha256'])
            self.assertEqual(body.decode(), '合成披露正文')
            ranged = [e for e in source.log if e['headers'].get('range')]
            self.assertTrue(ranged)
            self.assertEqual(ranged[0]['headers'].get('if-range'), hdrs['etag'])
            self.assertEqual(self.store.partials(), [])

    def test_weak_etag_partial_is_discarded(self):
        with running_source() as (base, source):
            post_advance(base, {'phase': 1})
            final_url, status, hdrs, wire, _ = httpclient.get(base + '/document', ZH)
            hdrs = {k.lower(): v for k, v in hdrs}
            self.assertTrue(hdrs['etag'].startswith('W/'))
            key = self.seed_partial(final_url, ZH, wire, hdrs['etag'],
                                    normalize_vary(hdrs['vary']), len(wire) // 2)
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'downloaded')
            events = {e['event']: e for e in self.store.journal_entries()}
            self.assertEqual(events['partial_discarded']['reason'],
                             'unverifiable-fragment')
            self.assertIsNone(self.store.partial_get(key))

    def test_if_range_mismatch_fetches_full_body(self):
        with running_source() as (base, source):
            final_url, status, hdrs, wire, _ = httpclient.get(base + '/document', ZH)
            hdrs = {k.lower(): v for k, v in hdrs}
            self.seed_partial(final_url, ZH, wire, hdrs['etag'],
                              normalize_vary(hdrs['vary']), len(wire) // 2)
            post_advance(base, {'phase': 2})  # 内容修订，强验证器变化
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'downloaded')
            entry = self.store.cache_get(result['cache_key'])
            body = self.store.cas_read(entry['body_ref']['sha256'])
            self.assertIn('修订'.encode(), body)

    def test_416_with_complete_fragment_commits_after_identity_check(self):
        with running_source() as (base, source):
            final_url, status, hdrs, wire, _ = httpclient.get(base + '/document', ZH)
            hdrs = {k.lower(): v for k, v in hdrs}
            # 崩溃发生在提交前：残片已是完整表示，但没有任何登记
            self.seed_partial(final_url, ZH, wire, hdrs['etag'],
                              normalize_vary(hdrs['vary']), len(wire))
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'resumed')
            self.assertEqual(result['bytes_downloaded'], 0)
            entry = self.store.cache_get(result['cache_key'])
            body = self.store.cas_read(entry['body_ref']['sha256'])
            self.assertEqual(body.decode(), '合成披露正文')
            self.assertEqual(self.store.partials(), [])

    def test_416_after_commit_reconciles_leftover_partial(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            self.collector.claim(sub)
            entry = self.store.cache_entries()[0]
            wire = self.store.cas_read(entry['wire_ref']['sha256'])
            # 崩溃发生在登记之后、清理残片之前
            self.seed_partial(entry['url'], ZH, wire, entry['etag'],
                              tuple(entry['vary']), len(wire))
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'revalidated')
            events = [e['event'] for e in self.store.journal_entries()]
            self.assertIn('partial_reconciled', events)
            self.assertEqual(self.store.partials(), [])

    # ---- 恢复核实 ----

    def test_repair_restores_body_from_wire_bytes(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            self.collector.claim(sub)
            entry = self.store.cache_entries()[0]
            os.remove(self.store.cas_path_for(entry['body_ref']['sha256']))
            report = self.collector.repair(verify_partials=False)
            self.assertEqual(report['bodies_restored'], [entry['cache_key']])
            restored = self.store.cache_get(entry['cache_key'])
            self.assertTrue(self.collector._entry_content_ok(restored))

    def test_repair_quarantines_unmatched_registration(self):
        with running_source() as (base, source):
            sub = self.collector.subscribe('小李', base + '/document', ZH)
            self.collector.claim(sub)
            entry = self.store.cache_entries()[0]
            for ref in ('wire_ref', 'body_ref'):
                os.remove(self.store.cas_path_for(entry[ref]['sha256']))
            report = self.collector.repair(verify_partials=False)
            self.assertEqual(report['entries_quarantined'], [entry['cache_key']])
            self.assertIsNone(self.store.cache_get(entry['cache_key']))
            # 隔离后刷新走无条件重取，不会停在条件验证里
            result = self.collector.refresh_subscription(sub)
            self.assertEqual(result['outcome'], 'downloaded')
            self.assertTrue(self.collector._entry_content_ok(
                self.store.cache_entries()[0]))


class FaultServerCase(unittest.TestCase):
    """故障注入：空响应与截断传输都不能变成“成功”。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='collector-fault-')
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.store = Store(Path(self.tmp) / 'store')
        self.collector = Collector(self.store)
        self.full = bytes(b % 251 for b in range(200))
        case = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def do_GET(self):
                if self.path == '/empty':
                    self.send_response(200)
                    self.send_header('ETag', '"empty-1"')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                if self.path == '/flaky':
                    etag = '"flaky-1"'
                    if (self.headers.get('If-Range') == etag
                            and self.headers.get('Range', '').startswith('bytes=')):
                        start = int(self.headers['Range'][6:].rstrip('-'))
                        body = case.full[start:]
                        self.send_response(206)
                        self.send_header('ETag', etag)
                        self.send_header('Content-Range',
                                         f'bytes {start}-199/200')
                        self.send_header('Content-Length', str(len(body)))
                        self.end_headers()
                        self.wfile.write(body)
                        return
                    # 声称 200 字节，只给 120 就断开
                    self.send_response(200)
                    self.send_header('ETag', etag)
                    self.send_header('Content-Length', '200')
                    self.end_headers()
                    self.wfile.write(case.full[:120])
                    self.wfile.flush()
                    self.close_connection = True
                    return
                self.send_response(404)
                self.send_header('Content-Length', '0')
                self.end_headers()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.base = f'http://127.0.0.1:{self.server.server_port}'

    def test_empty_response_is_refused(self):
        sub = self.collector.subscribe('小李', self.base + '/empty', {})
        with self.assertRaises(EmptyDeliveryError):
            self.collector.claim(sub)
        self.assertEqual(self.store.cache_entries(), [])
        self.assertEqual(self.store.deliveries(), [])
        events = [e['event'] for e in self.store.journal_entries()]
        self.assertIn('empty_refused', events)

    def test_truncated_download_resumes_and_never_commits_partial(self):
        sub = self.collector.subscribe('小李', self.base + '/flaky', {})
        with self.assertRaises(CollectorError):
            self.collector.claim(sub)
        # 截断件没有变成“成功”：没有登记、没有交付
        self.assertEqual(self.store.cache_entries(), [])
        self.assertEqual(self.store.deliveries(), [])
        partials = self.store.partials()
        self.assertEqual(len(partials), 1)
        self.assertEqual(partials[0]['have'], 120)
        # 再次领取：续传核实身份后补完
        delivery_id = self.collector.claim(sub)
        delivery = self.store.delivery_get(delivery_id)
        body = self.store.cas_read(delivery['body_ref']['sha256'])
        self.assertEqual(body, self.full)
        self.assertEqual(self.store.partials(), [])
        events = [e['event'] for e in self.store.journal_entries()]
        self.assertIn('download_interrupted', events)
        self.assertIn('resumed', events)


if __name__ == '__main__':
    unittest.main()
