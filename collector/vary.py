"""Vary 与缓存键：响应身份由 URL、协商请求头和来源当时的 Vary 共同决定。

来源改变 Vary（契约阶段 2 扩展到 X-Disclosure-Scope）后，旧条目的
vary_key 与新请求算出的键不同，自然失配；历史条目仍在 store 里可回查，
但不会被当作新请求的回答。
"""
import json

# 参与内容协商、需要纳入缓存键的请求头（小写）。
NEGOTIATED_HEADERS = ('accept-language', 'accept-encoding', 'x-disclosure-scope')


def normalize_vary(value):
    """把 Vary 头解析为去重、排序后的小写字段名元组。"""
    if not value:
        return ()
    return tuple(sorted({part.strip().lower() for part in value.split(',') if part.strip()}))


def vary_key(vary_fields, request_headers):
    """按来源声明的 Vary 字段，从本次请求头取对应值组成键。

    键里同时带上 vary 字段列表本身：来源扩展 Vary 后，即使旧字段取值
    相同，键也不同，旧缓存不会误中。
    """
    lowered = {k.lower(): v for k, v in request_headers.items()}
    pairs = tuple((field, lowered.get(field, '')) for field in vary_fields)
    return (vary_fields, pairs)


def cache_key(final_url, vary_fields, request_headers):
    """缓存条目身份：最终 URL（转址后）+ 当时 Vary 下的请求取值。"""
    key = vary_key(tuple(vary_fields), request_headers)
    return final_url + '|' + json.dumps(key, ensure_ascii=False)


def request_fingerprint(request_headers):
    """登记用：请求中实际出现的协商头快照（不含凭据）。"""
    lowered = {k.lower(): v for k, v in request_headers.items()}
    return {name: lowered[name] for name in NEGOTIATED_HEADERS if name in lowered}
