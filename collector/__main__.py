"""采集端命令行：订阅、刷新、领取、回查与恢复。

示例：
    python -m collector --store collector_data subscribe \
        --staff 小李 --url http://127.0.0.1:8765/document --lang zh --encoding gzip
    python -m collector --store collector_data refresh
    python -m collector --store collector_data claim --id <订阅ID>
    python -m collector --store collector_data report
    python -m collector --store collector_data repair
"""
import argparse
import json
import sys

from .service import Collector, CollectorError
from .store import Store


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=1))


def _build_parser():
    parser = argparse.ArgumentParser(prog='collector', description='多语种披露原件采集端')
    parser.add_argument('--store', default='collector_data', help='采集端保存目录（默认 collector_data）')
    parser.add_argument('--credential', action='append', default=[],
                        help='按 origin 授权的凭据，形如 ORIGIN=Authorization头值，可重复；'
                             '只发往同 origin，跨站转址不跟随')
    parser.add_argument('--timeout', type=float, default=30)
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('subscribe', help='登记一个资料员的领取订阅')
    p.add_argument('--staff', required=True, help='资料员')
    p.add_argument('--url', required=True, help='披露地址或下载别名')
    p.add_argument('--lang', help='Accept-Language，如 zh / en')
    p.add_argument('--encoding', help='Accept-Encoding，如 gzip / identity')
    p.add_argument('--scope', help='X-Disclosure-Scope 披露范围')
    p.add_argument('--header', action='append', default=[], help='额外请求头 "Name: value"')
    p.add_argument('--name', default='', help='订阅备注')

    p = sub.add_parser('unsubscribe', help='取消订阅（不影响他人已领取的原件）')
    p.add_argument('--id', required=True)

    p = sub.add_parser('refresh', help='按条件刷新订阅（默认全部）')
    p.add_argument('--id', help='只刷新指定订阅')

    p = sub.add_parser('claim', help='领取可读取原件并写交付件')
    p.add_argument('--id', required=True)

    p = sub.add_parser('deliveries', help='查看交付件')
    p.add_argument('--id', help='指定交付件 ID')

    p = sub.add_parser('history', help='查看刷新记录（辨认复用确认与重新下载）')
    p.add_argument('--id', help='按订阅过滤')

    sub.add_parser('report', help='按订阅汇总最近一次刷新结果')

    p = sub.add_parser('repair', help='核实登记与文件对应关系并恢复')
    p.add_argument('--no-verify', action='store_true', help='不联机核实部分下载')
    return parser


def _headers_from(args):
    headers = {}
    if args.lang:
        headers['Accept-Language'] = args.lang
    if args.encoding:
        headers['Accept-Encoding'] = args.encoding
    if args.scope:
        headers['X-Disclosure-Scope'] = args.scope
    for item in args.header:
        name, sep, value = item.partition(':')
        if not sep:
            raise CollectorError(f'无法解析请求头: {item}')
        headers[name.strip()] = value.strip()
    return headers


def main(argv=None):
    args = _build_parser().parse_args(argv)
    credentials = {}
    for item in args.credential:
        origin, sep, value = item.partition('=')
        if not sep:
            print('凭据格式应为 ORIGIN=值', file=sys.stderr)
            return 2
        credentials[origin.strip()] = value.strip()
    collector = Collector(Store(args.store), credentials=credentials, timeout=args.timeout)
    try:
        if args.command == 'subscribe':
            sid = collector.subscribe(args.staff, args.url, headers=_headers_from(args),
                                      name=args.name)
            _print({'subscription_id': sid})
        elif args.command == 'unsubscribe':
            _print({'removed': collector.unsubscribe(args.id)})
        elif args.command == 'refresh':
            if args.id:
                _print(collector.refresh_subscription(args.id))
            else:
                _print(collector.refresh_all())
        elif args.command == 'claim':
            delivery_id = collector.claim(args.id)
            _print(collector.store.delivery_get(delivery_id))
        elif args.command == 'deliveries':
            if args.id:
                _print(collector.store.delivery_get(args.id))
            else:
                _print(collector.store.deliveries())
        elif args.command == 'history':
            entries = collector.store.journal_entries()
            if args.id:
                entries = [e for e in entries if e.get('subscription_id') == args.id]
            _print(entries)
        elif args.command == 'report':
            _print(collector.refresh_report())
        elif args.command == 'repair':
            _print(collector.repair(verify_partials=not args.no_verify))
    except CollectorError as exc:
        print(f'采集失败: {exc}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
