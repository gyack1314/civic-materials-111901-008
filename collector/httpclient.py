"""采集端 HTTP 客户端：重定向链、跨 origin 凭据控制与流式读取。

只依赖标准库。本层是纯传输层，不做缓存判断；条件请求头、部分文件身份
由调用方（service）决定。所有响应体都经 on_response 回调流式交给调用方，
传输中断时调用方手里已落盘的部分就是恢复依据。
"""
import http.client
from urllib.parse import urljoin, urlsplit

# 301/302/303 一律按 GET 跟随（采集端只发 GET）；307/308 保持原方法。
REDIRECT_STATUSES = {301, 302, 303, 307, 308}
MAX_REDIRECTS = 10


class TransportError(Exception):
    """网络层失败（连接、截断、读超时等），调用方据此保留部分下载并重试。"""


def _origin(url):
    parts = urlsplit(url)
    return parts.hostname, parts.port or (443 if parts.scheme == 'https' else 80)


def same_origin(url_a, url_b):
    """授权范围判断：scheme+host+port 完全一致才允许沿用凭据。"""
    a, b = urlsplit(url_a), urlsplit(url_b)
    return a.scheme == b.scheme and _origin(url_a) == _origin(url_b)


def _header(headers, name):
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def _sensitive(header_names):
    return sorted(n for n in header_names if n.lower() in ('authorization', 'proxy-authorization', 'cookie'))


def _open(url, headers, timeout):
    parts = urlsplit(url)
    if parts.scheme != 'http':
        raise TransportError(f'仅支持 http 来源: {url}')
    target = parts.path or '/'
    if parts.query:
        target += '?' + parts.query
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=timeout)
    try:
        conn.request('GET', target, headers=dict(headers))
    except UnicodeEncodeError as exc:
        conn.close()
        raise TransportError(f'请求头含非 latin-1 字符，无法发送: {exc}') from exc
    return conn, conn.getresponse()


def get(url, headers=None, timeout=30, max_redirects=MAX_REDIRECTS, on_response=None):
    """GET 并跟随重定向，返回 (最终 URL, 状态, 响应头列表, 字节, 转址链)。

    on_response(final_url, status, resp_headers) 在最终响应头到达时调用；
    返回一个 chunk 写入函数则响应体流式交出（此时返回的字节为 b''），
    返回 None 则整体读入。转址链每项记录 {from, status, to, credentials}；
    跨 origin 跳转时丢弃授权类请求头并在链里标记，供交付件回查。
    """
    headers = dict(headers or {})
    chain = []
    current = url
    for _ in range(max_redirects + 1):
        conn = None
        try:
            conn, response = _open(current, headers, timeout)
            status = response.status
            resp_headers = list(response.getheaders())
            location = _header(resp_headers, 'location')
            if status in REDIRECT_STATUSES and location:
                response.read()  # 抽干转址响应体
                target = urljoin(current, location)
                hop = {'from': current, 'status': status, 'to': target}
                if same_origin(current, target):
                    hop['credentials'] = 'kept'
                else:
                    dropped = _sensitive(headers)
                    for name in dropped:
                        headers.pop(name, None)
                    marker = ','.join(n.lower() for n in dropped)
                    hop['credentials'] = 'dropped:' + marker if dropped else 'dropped:none'
                chain.append(hop)
                current = target
                continue
            handler = on_response(current, status, resp_headers) if on_response else None
            if handler is None:
                body = response.read()
            else:
                body = b''
                while True:
                    chunk = response.read(65536)
                    if not chunk:
                        break
                    handler(chunk)
            return current, status, resp_headers, body, chain
        except (OSError, http.client.HTTPException) as exc:
            raise TransportError(f'请求 {current} 失败: {exc}') from exc
        finally:
            if conn is not None:
                conn.close()
    raise TransportError(f'重定向次数超过 {max_redirects}: {url}')


def parse_content_range_total(resp_headers):
    """从 Content-Range 取当前完整长度（206 与 416 都有）；取不到返回 None。"""
    value = _header(resp_headers, 'content-range')
    if value and value.startswith('bytes '):
        tail = value[len('bytes '):]
        if '/' in tail:
            total = tail.rsplit('/', 1)[1]
            if total.isdigit():
                return int(total)
    return None


def parse_content_range_start(resp_headers):
    """从 206 的 Content-Range: bytes START-END/TOTAL 取起始偏移。"""
    value = _header(resp_headers, 'content-range')
    if value and value.startswith('bytes '):
        try:
            return int(value[len('bytes '):].split('-')[0])
        except (ValueError, IndexError):
            return None
    return None
