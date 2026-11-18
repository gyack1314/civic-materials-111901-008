"""采集端本地存储：内容寻址原件、缓存登记、交付件、订阅与刷新日志。

布局（root 由采集端独立管理，不随来源进程）：

- cas/<algo>/<hex>        内容寻址字节（传输字节与解码正文各自入库）
- cache/<key>.json        缓存登记（表示身份、验证器、存储引用、回查信息）
- deliveries/<id>.json    每次领取的交付件（请求条件、时刻、转址链）
- subscriptions/<id>.json 资料员订阅
- partials/<key>.json     未完成的断点续传状态（字节在同名 .bin）
- outbox/<id>             领取时物化的可读取原件（解码正文）
- quarantine/<key>.json   与文件对不上的登记，隔离后不再回答请求
- journal/refresh.jsonl   刷新记录：每次命中、重验、重下、修复都留痕
- tmp/                    写入暂存，os.replace 原子落位

文件保存与缓存登记之间崩溃时，repair 以“登记引用 ↔ 文件存在且摘要一致”
双向核实，孤儿内容保留在 cas 中可回查，不静默复用。
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid

SCHEMA = 1


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def _now():
    return time.time()


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.lock = threading.RLock()
        for name in ('cas', 'cache', 'deliveries', 'subscriptions', 'partials',
                     'journal', 'tmp', 'outbox', 'quarantine'):
            (self.root / name).mkdir(parents=True, exist_ok=True)

    # ---- 原子写 ----

    def _atomic_write(self, path, data):
        path = Path(path)
        fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix='.tmp-')
        try:
            with os.fdopen(fd, 'wb') as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
            dirfd = os.open(str(path.parent), os.O_DIRECTORY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _write_json(self, path, obj):
        self._atomic_write(path, (json.dumps(obj, ensure_ascii=False, indent=1) + '\n').encode('utf-8'))

    def write_bytes(self, path, data):
        """公共原子写：outbox 物化件等。"""
        self._atomic_write(Path(path), data)

    @staticmethod
    def _read_json(path):
        with open(path, 'rb') as handle:
            return json.load(handle)

    # ---- 内容寻址存储 ----

    def _cas_path(self, digest):
        return self.root / 'cas' / 'sha256' / digest[:2] / digest

    def cas_put(self, data):
        """写入内容寻址存储，返回 sha256 摘要；同内容只存一份。"""
        digest = sha256_hex(data)
        path = self._cas_path(digest)
        if not path.exists():
            with self.lock:
                path.parent.mkdir(parents=True, exist_ok=True)
                if not path.exists():
                    self._atomic_write(path, data)
        return digest

    def cas_path_for(self, digest):
        path = self._cas_path(digest)
        return path if path.exists() else None

    def cas_read(self, digest):
        path = self._cas_path(digest)
        if not path.exists():
            raise FileNotFoundError(f'内容缺失: {digest}')
        return path.read_bytes()

    def cas_verify(self, digest):
        """核实内容存在且摘要与身份一致。"""
        path = self._cas_path(digest)
        if not path.exists():
            return False
        return sha256_hex(path.read_bytes()) == digest

    def cas_orphans(self):
        """没有任何登记引用的内容（崩溃残留），保留可回查、不自动清除。"""
        referenced = set()
        for entry in self.cache_entries():
            for key in ('wire_ref', 'body_ref'):
                ref = entry.get(key)
                if ref:
                    referenced.add(ref.get('sha256'))
        for delivery in self.deliveries():
            for key in ('wire_ref', 'body_ref'):
                ref = delivery.get(key)
                if ref:
                    referenced.add(ref.get('sha256'))
        orphans = []
        base = self.root / 'cas' / 'sha256'
        if base.exists():
            for path in base.glob('*/*'):
                if path.is_file() and path.name not in referenced:
                    orphans.append(path.name)
        return sorted(orphans)

    # ---- 缓存登记 ----

    @staticmethod
    def _entry_name(cache_key):
        return sha256_hex(cache_key.encode('utf-8'))

    def cache_put(self, cache_key, entry):
        entry = dict(entry)
        entry['cache_key'] = cache_key
        entry['schema'] = SCHEMA
        self._write_json(self.root / 'cache' / (self._entry_name(cache_key) + '.json'), entry)

    def cache_get(self, cache_key):
        path = self.root / 'cache' / (self._entry_name(cache_key) + '.json')
        if not path.exists():
            return None
        return self._read_json(path)

    def cache_entries(self):
        base = self.root / 'cache'
        return [self._read_json(p) for p in sorted(base.glob('*.json'))]

    def quarantine_entry(self, cache_key):
        """把与文件对不上的登记移出 cache/，留档但不再回答请求。"""
        src = self.root / 'cache' / (self._entry_name(cache_key) + '.json')
        if not src.exists():
            return False
        os.replace(src, self.root / 'quarantine' / src.name)
        return True

    # ---- 交付件 ----

    def delivery_put(self, record):
        record = dict(record)
        record.setdefault('delivery_id', uuid.uuid4().hex)
        record['schema'] = SCHEMA
        self._write_json(self.root / 'deliveries' / (record['delivery_id'] + '.json'), record)
        return record['delivery_id']

    def delivery_get(self, delivery_id):
        path = self.root / 'deliveries' / (delivery_id + '.json')
        if not path.exists():
            return None
        return self._read_json(path)

    def deliveries(self):
        base = self.root / 'deliveries'
        return [self._read_json(p) for p in sorted(base.glob('*.json'))]

    # ---- 订阅 ----

    def subscription_put(self, record):
        record = dict(record)
        record.setdefault('subscription_id', uuid.uuid4().hex)
        record.setdefault('created_at', _now())
        record['schema'] = SCHEMA
        self._write_json(self.root / 'subscriptions' / (record['subscription_id'] + '.json'), record)
        return record['subscription_id']

    def subscription_get(self, subscription_id):
        path = self.root / 'subscriptions' / (subscription_id + '.json')
        if not path.exists():
            return None
        return self._read_json(path)

    def subscriptions(self):
        base = self.root / 'subscriptions'
        return [self._read_json(p) for p in sorted(base.glob('*.json'))]

    def subscription_cancel(self, subscription_id):
        path = self.root / 'subscriptions' / (subscription_id + '.json')
        if not path.exists():
            return False
        path.unlink()
        return True

    # ---- 断点续传状态 ----

    def partial_bin_path(self, cache_key):
        return self.root / 'partials' / (self._entry_name(cache_key) + '.bin')

    def partial_put(self, cache_key, state):
        state = dict(state)
        state['cache_key'] = cache_key
        self._write_json(self.root / 'partials' / (self._entry_name(cache_key) + '.json'), state)

    def partial_get(self, cache_key):
        path = self.root / 'partials' / (self._entry_name(cache_key) + '.json')
        if not path.exists():
            return None
        return self._read_json(path)

    def partials(self):
        base = self.root / 'partials'
        return [self._read_json(p) for p in sorted(base.glob('*.json'))]

    def partial_clear(self, cache_key):
        path = self.root / 'partials' / (self._entry_name(cache_key) + '.json')
        if path.exists():
            path.unlink()
        bin_path = self.partial_bin_path(cache_key)
        if bin_path.exists():
            bin_path.unlink()

    # ---- 领取物化 ----

    def outbox_path(self, delivery_id):
        return self.root / 'outbox' / delivery_id

    # ---- 刷新记录 ----

    def journal(self, event):
        """追加一条刷新/修复记录，资料员据此辨认复用与重下。"""
        event = dict(event)
        event.setdefault('time', _now())
        event.setdefault('event_id', uuid.uuid4().hex)
        line = (json.dumps(event, ensure_ascii=False) + '\n').encode('utf-8')
        with self.lock:
            with open(self.root / 'journal' / 'refresh.jsonl', 'ab') as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
        return event

    def journal_entries(self):
        path = self.root / 'journal' / 'refresh.jsonl'
        if not path.exists():
            return []
        out = []
        with open(path, 'rb') as handle:
            for line in handle:
                line = line.strip()
                if line:
                    out.append(json.loads(line))
        return out
