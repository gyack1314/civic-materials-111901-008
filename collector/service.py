"""采集服务：订阅、条件刷新、断点续传、领取交付与恢复核实。

设计要点（对应 docs/source-contract.md）：

- 每个表示的身份 = 最终 URL + 来源当时的 Vary + 协商请求头取值；
  验证器（ETag）只用于它验证过的那个表示，弱验证器不参与断点续传；
- 传输字节（wire，可能 gzip）与解码正文（body）分别做内容寻址登记，
  交付件同时引用两者，可回查原始传输内容；
- 每次领取先按条件刷新：304 且本地正文可用才算复用来源确认；
  304 但正文缺失、登记与文件脱节、部分下载身份不符，都转为无条件重取，
  绝不交付空件，也不会停在条件验证里出不来；
- 恢复（repair）双向核实“登记 ↔ 文件”，无法对应的登记隔离留档。
"""
import gzip
import os
import time
from datetime import datetime, timezone

from . import httpclient
from .store import Store
from .vary import cache_key as make_cache_key
from .vary import normalize_vary, request_fingerprint


class CollectorError(Exception):
    """采集失败（网络、来源状态、身份不符等）。"""


class EmptyDeliveryError(CollectorError):
    """来源给出空内容或本地无可用正文：拒绝登记与交付。"""


def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def _headers_dict(resp_headers):
    out = {}
    for key, value in resp_headers:
        out[key.lower()] = value
    return out


def _is_weak(etag):
    return bool(etag) and str(etag).startswith('W/')


class Collector:
    def __init__(self, store, credentials=None, timeout=30):
        self.store = store if isinstance(store, Store) else Store(store)
        # credentials: {origin URL: Authorization 头值}，只发往同 origin，不持久化
        self.credentials = dict(credentials or {})
        self.timeout = timeout

    # ---- 订阅 ----

    def subscribe(self, staff, url, headers=None, name=None):
        record = {
            'staff': staff,
            'url': url,
            'request_headers': dict(headers or {}),
            'name': name or '',
        }
        subscription_id = self.store.subscription_put(record)
        self.store.journal({
            'event': 'subscribe', 'subscription_id': subscription_id,
            'staff': staff, 'url': url,
            'request': request_fingerprint(record['request_headers']),
        })
        return subscription_id

    def unsubscribe(self, subscription_id):
        """只解除登记：交付件、缓存与共享内容保留，他人手中原件不受影响。"""
        removed = self.store.subscription_cancel(subscription_id)
        self.store.journal({
            'event': 'unsubscribe', 'subscription_id': subscription_id, 'removed': removed,
        })
        return removed

    # ---- 刷新 ----

    def refresh_all(self):
        results = []
        for sub in self.store.subscriptions():
            try:
                results.append(self._refresh(sub))
            except CollectorError as exc:
                results.append({
                    'outcome': 'error', 'error': str(exc),
                    'subscription_id': sub.get('subscription_id'),
                })
        return results

    def refresh_subscription(self, subscription_id):
        sub = self.store.subscription_get(subscription_id)
        if sub is None:
            raise CollectorError(f'未知订阅: {subscription_id}')
        return self._refresh(sub)

    def _refresh(self, sub):
        url = sub['url']
        req_headers = dict(sub.get('request_headers') or {})
        req_headers.update(self._auth_headers(url))
        fetched_at = time.time()

        # 1) 有未完成下载时，先核实残片与当前响应的字节身份
        partial = self._find_partial(url, req_headers)
        if partial is not None:
            resumed = self._resume_partial(partial, sub, req_headers, fetched_at)
            if resumed is not None:
                return resumed

        # 2) 按缓存条目（匹配的表示）发条件请求
        entry = self._match_entry(url, req_headers)
        conditional = {}
        if entry is not None and entry.get('etag'):
            conditional['If-None-Match'] = entry['etag']
        outcome = self._fetch(sub, req_headers, conditional, fetched_at,
                              note='conditional' if conditional else 'initial')
        if outcome['status'] == 304:
            return self._on_not_modified(entry, sub, req_headers, outcome, fetched_at)
        return outcome['result']

    def _auth_headers(self, url):
        """凭据按 origin 范围附加；跨 origin 转址由 httpclient 丢弃。"""
        headers = {}
        for origin, value in self.credentials.items():
            if httpclient.same_origin(url, origin):
                headers['Authorization'] = value
        return headers

    # ---- 缓存条目与部分下载的匹配 ----

    @staticmethod
    def _rank(record):
        return (len(record.get('vary') or ()), record.get('stored_at', 0),
                record.get('updated_at', 0))

    def _match_entry(self, url, req_headers):
        """按 (URL, 当前请求头, 条目当时的 Vary) 选出可用于条件请求的条目。"""
        best = None
        for entry in self.store.cache_entries():
            if entry.get('url') != url and url not in entry.get('requested_via', []):
                continue
            key = make_cache_key(entry['url'], entry.get('vary') or (), req_headers)
            if key != entry.get('cache_key'):
                continue
            if best is None or self._rank(entry) > self._rank(best):
                best = entry
        return best

    def _find_partial(self, url, req_headers):
        best = None
        for partial in self.store.partials():
            if partial.get('url') != url and url not in partial.get('requested_via', []):
                continue
            key = make_cache_key(partial['url'], partial.get('vary') or (), req_headers)
            if key != partial.get('cache_key'):
                continue
            if best is None or self._rank(partial) > self._rank(best):
                best = partial
        return best

    # ---- 一次 GET：200 流式落盘并登记，304/其他状态回报 ----

    def _fetch(self, sub, req_headers, extra_headers, fetched_at, note):
        url = sub['url']
        headers = dict(req_headers)
        headers.update(extra_headers)
        holder = {}

        def on_response(final_url, status, resp_headers):
            if status != 200:
                return None
            return self._open_partial(holder, sub, req_headers, final_url, resp_headers)

        try:
            final_url, status, resp_headers, body, chain = httpclient.get(
                url, headers, timeout=self.timeout, on_response=on_response)
        except httpclient.TransportError as exc:
            self._interrupt_partial(holder, str(exc))
            self.store.journal({
                'event': 'fetch_failed', 'subscription_id': sub.get('subscription_id'),
                'url': url, 'error': str(exc), 'note': note,
            })
            raise CollectorError(str(exc)) from exc

        if status == 200:
            result = self._commit_holder(holder, sub, req_headers, final_url,
                                         resp_headers, chain, fetched_at, note)
            return {'status': 200, 'result': result}
        if status == 304:
            return {'status': 304, 'final_url': final_url, 'chain': chain,
                    'resp': _headers_dict(resp_headers)}
        self.store.journal({
            'event': 'fetch_failed', 'subscription_id': sub.get('subscription_id'),
            'url': url, 'status': status, 'note': note,
        })
        raise CollectorError(f'来源返回 {status}: {url}')

    def _open_partial(self, holder, sub, req_headers, final_url, resp_headers):
        """200 响应头到达：建立部分下载状态，返回流式写入函数。"""
        hdrs = _headers_dict(resp_headers)
        vary = normalize_vary(hdrs.get('vary'))
        key = make_cache_key(final_url, vary, req_headers)
        etag = hdrs.get('etag')
        length = hdrs.get('content-length')
        state = {
            'cache_key': key,
            'url': final_url,
            'requested_via': [sub['url']],
            'etag': etag,
            'weak': _is_weak(etag),
            'resumable': bool(etag) and not _is_weak(etag),
            'vary': list(vary),
            'request_fingerprint': request_fingerprint(req_headers),
            'total': int(length) if length and length.isdigit() else None,
            'have': 0,
            'updated_at': time.time(),
        }
        self.store.partial_put(key, state)
        handle = open(self.store.partial_bin_path(key), 'wb')
        holder.update(kind='partial', cache_key=key, handle=handle,
                      state=state, final_url=final_url, resp=hdrs, have=0)

        def write(chunk):
            handle.write(chunk)
            holder['have'] += len(chunk)

        return write

    def _interrupt_partial(self, holder, error):
        """传输中断：保留已落盘部分，登记实际字节数，等待续传核实。"""
        handle = holder.get('handle')
        if handle is not None and not handle.closed:
            try:
                handle.close()
            except OSError:
                pass
        key = holder.get('cache_key')
        if not key:
            return
        bin_path = self.store.partial_bin_path(key)
        have = bin_path.stat().st_size if bin_path.exists() else 0
        state = self.store.partial_get(key) or holder.get('state') or {}
        state['have'] = have
        state['updated_at'] = time.time()
        state['last_error'] = error
        self.store.partial_put(key, state)
        self.store.journal({
            'event': 'download_interrupted', 'cache_key': key,
            'have': have, 'total': state.get('total'), 'error': error,
        })

    def _commit_holder(self, holder, sub, req_headers, final_url, resp_headers,
                       chain, fetched_at, note):
        """200 流式读完：核对长度，提交登记；空件与截断件都拒绝。"""
        handle = holder.get('handle')
        if handle is not None and not handle.closed:
            handle.close()
        state = holder['state']
        key = state['cache_key']
        bin_path = self.store.partial_bin_path(key)
        have = bin_path.stat().st_size if bin_path.exists() else 0
        total = state.get('total')
        if total is not None and have != total:
            self._interrupt_partial(holder, f'长度不符: {have}/{total}')
            raise CollectorError(f'传输截断 {have}/{total}: {final_url}')
        wire = bin_path.read_bytes() if bin_path.exists() else b''
        if not wire:
            self.store.partial_clear(key)
            self.store.journal({
                'event': 'empty_refused', 'subscription_id': sub.get('subscription_id'),
                'url': sub['url'], 'final_url': final_url, 'note': note,
            })
            raise EmptyDeliveryError(f'来源返回空内容，拒绝登记: {final_url}')
        try:
            result = self._commit_wire(key, wire, holder['resp'], sub, req_headers,
                                       final_url, chain, fetched_at, note,
                                       outcome='downloaded')
        except Exception:
            self.store.partial_clear(key)
            raise
        self.store.partial_clear(key)
        return result

    def _commit_wire(self, key, wire, resp, sub, req_headers, final_url, chain,
                     fetched_at, note, outcome, transferred=None):
        """两阶段提交：内容入库并核实 → 缓存登记并回读核实。"""
        encoding = resp.get('content-encoding', 'identity')
        body = self._decode(encoding, wire)
        if not body:
            self.store.journal({
                'event': 'empty_refused', 'subscription_id': sub.get('subscription_id'),
                'url': sub['url'], 'final_url': final_url, 'note': note,
            })
            raise EmptyDeliveryError(f'解码后正文为空，拒绝登记: {final_url}')
        wire_ref = {'sha256': self.store.cas_put(wire), 'size': len(wire)}
        body_ref = {'sha256': self.store.cas_put(body), 'size': len(body)}
        if not (self.store.cas_verify(wire_ref['sha256'])
                and self.store.cas_verify(body_ref['sha256'])):
            raise CollectorError('内容写入后核实失败: ' + key)
        etag = resp.get('etag')
        now = time.time()
        existing = self.store.cache_get(key) or {}
        requested_via = sorted(set(existing.get('requested_via', [])) | {sub['url']})
        entry = {
            'url': final_url,
            'requested_via': requested_via,
            'vary': list(normalize_vary(resp.get('vary'))),
            'etag': etag,
            'weak': _is_weak(etag),
            'content_encoding': encoding,
            'content_language': resp.get('content-language'),
            'content_type': resp.get('content-type'),
            'wire_ref': wire_ref,
            'body_ref': body_ref,
            'request_fingerprint': request_fingerprint(req_headers),
            'stored_at': existing.get('stored_at', now),
            'last_validated_at': now,
        }
        self.store.cache_put(key, entry)
        check = self.store.cache_get(key)
        if not check or check.get('wire_ref') != wire_ref or check.get('body_ref') != body_ref:
            raise CollectorError('缓存登记回读核实失败: ' + key)
        event = self.store.journal({
            'event': outcome, 'subscription_id': sub.get('subscription_id'),
            'staff': sub.get('staff'), 'url': sub['url'], 'final_url': final_url,
            'cache_key': key, 'etag': etag, 'weak': _is_weak(etag),
            'reused_source_confirmation': False,
            'bytes_downloaded': len(wire) if transferred is None else transferred,
            'note': note, 'chain': chain,
            'fetched_at': fetched_at, 'fetched_at_iso': _iso(fetched_at),
        })
        return {
            'outcome': outcome, 'reused_source_confirmation': False,
            'bytes_downloaded': len(wire) if transferred is None else transferred,
            'cache_key': key, 'final_url': final_url, 'chain': chain,
            'etag': etag, 'weak': _is_weak(etag), 'fetched_at': fetched_at,
            'event_id': event['event_id'],
            'subscription_id': sub.get('subscription_id'),
            'content_language': entry['content_language'],
        }

    # ---- 304：来源确认未变 ----

    def _on_not_modified(self, entry, sub, req_headers, outcome, fetched_at):
        if entry is not None and self._entry_content_ok(entry):
            entry['last_validated_at'] = time.time()
            via = set(entry.get('requested_via', [])) | {sub['url']}
            entry['requested_via'] = sorted(via)
            self.store.cache_put(entry['cache_key'], entry)
            event = self.store.journal({
                'event': 'revalidated', 'subscription_id': sub.get('subscription_id'),
                'staff': sub.get('staff'), 'url': sub['url'],
                'final_url': outcome['final_url'], 'cache_key': entry['cache_key'],
                'etag': entry.get('etag'), 'weak': entry.get('weak'),
                'reused_source_confirmation': True, 'bytes_downloaded': 0,
                'chain': outcome['chain'],
                'fetched_at': fetched_at, 'fetched_at_iso': _iso(fetched_at),
            })
            return {
                'outcome': 'revalidated', 'reused_source_confirmation': True,
                'bytes_downloaded': 0, 'cache_key': entry['cache_key'],
                'final_url': outcome['final_url'], 'chain': outcome['chain'],
                'etag': entry.get('etag'), 'weak': entry.get('weak'),
                'fetched_at': fetched_at, 'event_id': event['event_id'],
                'subscription_id': sub.get('subscription_id'),
                'content_language': entry.get('content_language'),
            }
        # 304 但本地没有可用正文：不能发布空件，也不能继续条件验证——无条件重取
        self.store.journal({
            'event': 'revalidate_missing_body',
            'subscription_id': sub.get('subscription_id'), 'url': sub['url'],
            'cache_key': entry.get('cache_key') if entry else None,
            'etag': entry.get('etag') if entry else None,
        })
        retry = self._fetch(sub, req_headers, {}, fetched_at, note='304-but-body-missing')
        if retry['status'] != 200:
            raise CollectorError('无条件重取未获 200，无法恢复正文')
        return retry['result']

    # ---- 断点续传与 416 恢复 ----

    def _resume_partial(self, partial, sub, req_headers, fetched_at):
        key = partial['cache_key']
        bin_path = self.store.partial_bin_path(key)
        have = bin_path.stat().st_size if bin_path.exists() else 0
        etag = partial.get('etag')
        if have == 0 or not etag or partial.get('weak') or not partial.get('resumable'):
            # 弱验证器或缺失验证器的残片无法核实身份，丢弃重下
            self._drop_partial(key, 'unverifiable-fragment')
            return None
        headers = dict(req_headers)
        headers['If-Range'] = etag
        headers['Range'] = f'bytes={have}-'
        holder = {}

        def on_response(final_url, status, resp_headers):
            if status == 206:
                start = httpclient.parse_content_range_start(resp_headers)
                if start != have:
                    raise httpclient.TransportError(
                        f'206 起始偏移 {start} 与本地部分 {have} 不符')
                handle = open(bin_path, 'ab')
                holder.update(kind='resume', cache_key=key, handle=handle, got=0)

                def write(chunk):
                    handle.write(chunk)
                    holder['got'] += len(chunk)

                return write
            if status == 200:
                # 验证器已变化：旧部分作废，新表示重新流式落盘
                return self._open_partial(holder, sub, req_headers, final_url, resp_headers)
            return None

        try:
            final_url, status, resp_headers, body, chain = httpclient.get(
                sub['url'], headers, timeout=self.timeout, on_response=on_response)
        except httpclient.TransportError as exc:
            self._interrupt_partial(holder, str(exc))
            raise CollectorError(str(exc)) from exc

        if status == 206:
            if holder.get('handle') and not holder['handle'].closed:
                holder['handle'].close()
            got = holder.get('got', 0)
            total = httpclient.parse_content_range_total(resp_headers)
            new_have = have + got
            if total is not None and new_have == total:
                wire = bin_path.read_bytes()
                result = self._commit_wire(
                    key, wire, _headers_dict(resp_headers), sub, req_headers,
                    final_url, chain, fetched_at, note='resume',
                    outcome='resumed', transferred=got)
                self.store.partial_clear(key)
                return result
            partial['have'] = new_have
            partial['updated_at'] = time.time()
            self.store.partial_put(key, partial)
            self.store.journal({
                'event': 'download_interrupted', 'cache_key': key,
                'have': new_have, 'total': total,
            })
            raise CollectorError(f'续传后仍不完整: {new_have}/{total}')
        if status == 200:
            return self._commit_holder(holder, sub, req_headers, final_url,
                                       resp_headers, chain, fetched_at, 'resume-200')
        if status == 416:
            return self._recover_416(partial, sub, req_headers, have,
                                     resp_headers, chain, fetched_at)
        raise CollectorError(f'续传请求得到意外状态 {status}: {sub["url"]}')

    def _recover_416(self, partial, sub, req_headers, have, resp_headers,
                     chain, fetched_at):
        """416：本地部分不短于来源当前全长，按字节身份分别处理。"""
        key = partial['cache_key']
        total = httpclient.parse_content_range_total(resp_headers)
        entry = self.store.cache_get(key)
        if entry is not None and self._entry_content_ok(entry):
            # 崩溃发生在登记之后、清理残片之前：登记仍有效，清掉残片即可
            self._drop_partial(key, 'already-committed')
            event = self.store.journal({
                'event': 'partial_reconciled', 'cache_key': key,
                'subscription_id': sub.get('subscription_id'),
            })
            return {
                'outcome': 'revalidated', 'reused_source_confirmation': True,
                'bytes_downloaded': 0, 'cache_key': key, 'final_url': entry['url'],
                'chain': chain, 'etag': entry.get('etag'), 'weak': entry.get('weak'),
                'fetched_at': fetched_at, 'event_id': event['event_id'],
                'subscription_id': sub.get('subscription_id'),
                'content_language': entry.get('content_language'),
            }
        if total is not None and have == total:
            # 416 说明 If-Range 的强验证器仍成立：残片就是当前表示，直接提交
            bin_path = self.store.partial_bin_path(key)
            wire = bin_path.read_bytes()
            hdrs = _headers_dict(resp_headers)
            commit_key = make_cache_key(partial['url'], normalize_vary(hdrs.get('vary')),
                                        req_headers)
            result = self._commit_wire(
                commit_key, wire, hdrs, sub, req_headers, partial['url'], chain,
                fetched_at, note='416-confirmed', outcome='resumed', transferred=0)
            self.store.partial_clear(key)
            return result
        # 身份无法对应：残片作废，无条件重取
        self._drop_partial(key, f'stale-416 have={have} total={total}')
        retry = self._fetch(sub, req_headers, {}, fetched_at, note='partial-416')
        if retry['status'] != 200:
            raise CollectorError('416 后无条件重取未获 200')
        return retry['result']

    def _drop_partial(self, key, reason):
        self.store.partial_clear(key)
        self.store.journal({'event': 'partial_discarded', 'cache_key': key, 'reason': reason})

    # ---- 身份核实 ----

    @staticmethod
    def _decode(encoding, wire):
        if encoding and 'gzip' in encoding:
            try:
                return gzip.decompress(wire)
            except OSError:
                return b''
        return wire

    def _entry_content_ok(self, entry):
        wire = entry.get('wire_ref') or {}
        body = entry.get('body_ref') or {}
        if not wire.get('sha256') or not body.get('sha256'):
            return False
        if body.get('size', 0) <= 0:
            return False
        return (self.store.cas_verify(wire['sha256'])
                and self.store.cas_verify(body['sha256']))

    # ---- 领取交付 ----

    def claim(self, subscription_id):
        """领取可读取原件：先按条件刷新，再核实内容，最后写交付件。

        交付件记录本次请求的条件、取得时刻与完整转址链；以后别名改指向
        或来源改 Vary，都不影响这份记录的解释。
        """
        sub = self.store.subscription_get(subscription_id)
        if sub is None:
            raise CollectorError(f'未知订阅: {subscription_id}')
        req_headers = dict(sub.get('request_headers') or {})
        req_headers.update(self._auth_headers(sub['url']))
        result = self._refresh(sub)
        entry = self.store.cache_get(result['cache_key'])
        if entry is None or not self._entry_content_ok(entry):
            retry = self._fetch(sub, req_headers, {}, time.time(), note='claim-refetch')
            if retry['status'] != 200:
                raise EmptyDeliveryError('无法取得可用正文，拒绝交付')
            result = retry['result']
            entry = self.store.cache_get(result['cache_key'])
        if entry is None or not self._entry_content_ok(entry):
            raise EmptyDeliveryError('刷新后仍无可用正文，拒绝交付空件')
        body = self.store.cas_read(entry['body_ref']['sha256'])
        if not body:
            raise EmptyDeliveryError('正文为空，拒绝交付空件')
        now = time.time()
        delivery = {
            'subscription_id': subscription_id,
            'staff': sub.get('staff'),
            'requested_url': sub['url'],
            'final_url': entry['url'],
            'request_headers': request_fingerprint(req_headers),
            'fetched_at': result['fetched_at'],
            'fetched_at_iso': _iso(result['fetched_at']),
            'delivered_at': now,
            'delivered_at_iso': _iso(now),
            'chain': result['chain'],
            'vary': entry.get('vary'),
            'content_language': entry.get('content_language'),
            'content_encoding': entry.get('content_encoding'),
            'etag': entry.get('etag'),
            'weak': entry.get('weak'),
            'wire_ref': entry['wire_ref'],
            'body_ref': entry['body_ref'],
            'cache_key': entry['cache_key'],
            'source_outcome': result['outcome'],
            'refresh_event': result.get('event_id'),
        }
        delivery_id = self.store.delivery_put(delivery)
        outbox = self.store.outbox_path(delivery_id)
        self.store.write_bytes(outbox, body)
        delivery['delivery_id'] = delivery_id
        delivery['outbox'] = str(outbox)
        self.store.delivery_put(delivery)
        self.store.journal({
            'event': 'delivered', 'delivery_id': delivery_id,
            'subscription_id': subscription_id, 'staff': sub.get('staff'),
            'cache_key': entry['cache_key'],
            'body_sha256': entry['body_ref']['sha256'],
            'wire_sha256': entry['wire_ref']['sha256'],
            'source_outcome': result['outcome'],
        })
        return delivery_id

    # ---- 恢复核实 ----

    def repair(self, verify_partials=True):
        """核实存储一致性：登记 ↔ 文件、残片 ↔ 当前响应、交付件 ↔ 内容。"""
        report = {
            'entries_ok': 0, 'bodies_restored': [], 'entries_quarantined': [],
            'partials': [], 'deliveries_broken': [], 'orphans': [],
        }
        for entry in self.store.cache_entries():
            wire_ref = entry.get('wire_ref') or {}
            body_ref = entry.get('body_ref') or {}
            wire_ok = bool(wire_ref.get('sha256')) and self.store.cas_verify(wire_ref['sha256'])
            body_ok = bool(body_ref.get('sha256')) and self.store.cas_verify(body_ref['sha256'])
            if wire_ok and body_ok:
                report['entries_ok'] += 1
                continue
            if wire_ok and not body_ok:
                # 传输字节仍在：重新解码恢复正文登记
                wire = self.store.cas_read(wire_ref['sha256'])
                body = self._decode(entry.get('content_encoding'), wire)
                if body:
                    digest = self.store.cas_put(body)
                    entry['body_ref'] = {'sha256': digest, 'size': len(body)}
                    self.store.cache_put(entry['cache_key'], entry)
                    self.store.journal({
                        'event': 'body_restored', 'cache_key': entry['cache_key'],
                        'body_sha256': digest,
                    })
                    report['bodies_restored'].append(entry['cache_key'])
                    continue
            # 登记与文件无法对应：隔离留档，后续刷新会无条件重取
            self.store.quarantine_entry(entry['cache_key'])
            self.store.journal({
                'event': 'entry_quarantined', 'cache_key': entry['cache_key'],
                'wire_ok': wire_ok, 'body_ok': body_ok,
            })
            report['entries_quarantined'].append(entry['cache_key'])
        for partial in self.store.partials():
            report['partials'].append(self._repair_partial(partial, verify_partials))
        for delivery in self.store.deliveries():
            for ref_key in ('wire_ref', 'body_ref'):
                ref = delivery.get(ref_key) or {}
                if ref.get('sha256') and not self.store.cas_verify(ref['sha256']):
                    report['deliveries_broken'].append(delivery.get('delivery_id'))
        report['orphans'] = self.store.cas_orphans()
        self.store.journal({
            'event': 'repair',
            'summary': {k: (len(v) if isinstance(v, list) else v)
                        for k, v in report.items()},
        })
        return report

    def _repair_partial(self, partial, verify):
        key = partial['cache_key']
        bin_path = self.store.partial_bin_path(key)
        have = bin_path.stat().st_size if bin_path.exists() else 0
        info = {'cache_key': key, 'have': have, 'etag': partial.get('etag')}
        if have == 0:
            self._drop_partial(key, 'empty')
            info['action'] = 'dropped-empty'
            return info
        if not verify:
            info['action'] = 'pending'
            return info
        if partial.get('weak') or not partial.get('etag'):
            self._drop_partial(key, 'weak-validator')
            info['action'] = 'dropped-weak'
            return info
        # 用 If-Range 核实残片与当前响应的字节身份，能补完就补完
        req_headers = dict(partial.get('request_fingerprint') or {})
        req_headers.update(self._auth_headers(partial['url']))
        sub_like = {
            'url': partial['url'], 'request_headers': req_headers,
            'subscription_id': None, 'staff': None,
        }
        try:
            result = self._resume_partial(partial, sub_like, req_headers, time.time())
            info['action'] = 'completed' if result else 'requeued'
        except CollectorError as exc:
            info['action'] = f'error: {exc}'
        return info

    # ---- 刷新记录 ----

    def refresh_report(self):
        """按订阅汇总最近一次刷新：复用来源确认还是重新下载。"""
        latest = {}
        for event in self.store.journal_entries():
            sid = event.get('subscription_id')
            if sid and event.get('event') in ('revalidated', 'downloaded', 'resumed'):
                latest[sid] = event
        subs = {s['subscription_id']: s for s in self.store.subscriptions()}
        out = []
        for sid, event in latest.items():
            sub = subs.get(sid) or {}
            out.append({
                'subscription_id': sid,
                'staff': event.get('staff') or sub.get('staff'),
                'url': event.get('url'),
                'outcome': event['event'],
                'reused_source_confirmation': event.get('reused_source_confirmation'),
                'bytes_downloaded': event.get('bytes_downloaded'),
                'at': event.get('fetched_at_iso') or event.get('time'),
            })
        return out
