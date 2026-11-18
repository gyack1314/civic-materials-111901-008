"""本地合成资料来源，仅提供来源行为与观察记录。"""
import argparse
from contextlib import contextmanager
import gzip
import hashlib
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from pathlib import Path
import threading
from urllib.parse import parse_qs, urlsplit

DATA = Path(__file__).resolve().parents[1] / 'data'

class Source:
    def __init__(self):
        self.lock = threading.RLock()
        self.clock = 0
        self.phase = 0
        self.log = []
        self.extra = {}

    def response(self, method, raw_path, headers, body):
        path = urlsplit(raw_path).path
        q = {k:v[-1] for k,v in parse_qs(urlsplit(raw_path).query).items()}
        with self.lock:
            self.log.append({'method':method,'path':raw_path,'time':self.clock,
                             'headers':{k.lower():v for k,v in headers.items()}})
            if path == '/health': return 200, {}, {'来源':'本地合成资料','phase':self.phase,'time':self.clock}
            if path == '/__admin/log': return 200, {}, self.log[:-1]
            if path == '/__admin/advance' and method == 'POST':
                value=json.loads(body or b'{}')
                self.clock += int(value.get('seconds',0))
                self.phase = int(value.get('phase',self.phase))
                return 200, {}, {'phase':self.phase,'time':self.clock}
            if path.startswith('/__admin/'):
                return 404, {}, {'error':'未知演练控制'}
            return self.product(method,path,q,{k.lower():v for k,v in headers.items()},body)

    def product(self,method,path,q,h,body):
        if path=='/alias':return 301,{'Location':'/document' if self.phase<2 else '/other'},b''
        if path in ('/document','/other'):
            language='zh' if h.get('accept-language','zh').startswith('zh') else 'en'
            text=('合成披露正文' if language=='zh' else 'Synthetic disclosure')+(' 修订' if self.phase else '')+(' other' if path=='/other' else '')
            raw=text.encode();coding='gzip' if 'gzip' in h.get('accept-encoding','') else 'identity'
            wire=gzip.compress(raw,mtime=0) if coding=='gzip' else raw
            tag=('W/' if self.phase==1 else '')+'"'+hashlib.sha256(raw if self.phase==1 else wire).hexdigest()[:12]+'"'
            headers={'ETag':tag,'Vary':'Accept-Language, Accept-Encoding' if self.phase<2 else 'Accept-Language, Accept-Encoding, X-Disclosure-Scope','Content-Type':'text/plain; charset=utf-8','Content-Language':language,'Accept-Ranges':'bytes'}
            if coding=='gzip':headers['Content-Encoding']='gzip'
            if h.get('if-none-match')==tag:return 304,headers,b''
            rang=h.get('range','')
            if rang.startswith('bytes=') and (not h.get('if-range') or h['if-range']==tag and not tag.startswith('W/')):
                start=int(rang[6:].split('-')[0])
                if start>=len(wire):return 416,{**headers,'Content-Range':'bytes */'+str(len(wire))},b''
                return 206,{**headers,'Content-Range':f'bytes {start}-{len(wire)-1}/{len(wire)}'},wire[start:]
            return 200,headers,wire
        if path=='/external':
            port=h['host'].split(':')[-1];return 302,{'Location':f'http://localhost:{port}/document'},b''
        return 404,{}, {'error':'未知披露地址'}

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self,*args): pass
    def do_GET(self): self.handle_request()
    def do_POST(self): self.handle_request()
    def handle_request(self):
        size=int(self.headers.get('Content-Length','0'))
        try:
            status,headers,body=self.server.source.response(self.command,self.path,self.headers,self.rfile.read(size))
            if not isinstance(body,(str,bytes)):
                body=json.dumps(body,ensure_ascii=False).encode();headers.setdefault('Content-Type','application/json; charset=utf-8')
            elif isinstance(body,str):
                body=body.encode();headers.setdefault('Content-Type','text/html; charset=utf-8')
        except (ValueError,KeyError,TypeError) as exc:
            status,headers,body=400,{'Content-Type':'application/json'},json.dumps({'error':str(exc)},ensure_ascii=False).encode()
        if status==0:
            self.close_connection=True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.send_response(status)
        for k,v in headers.items():self.send_header(k,str(v))
        self.send_header('Content-Length',len(body))
        self.end_headers()
        if body:self.wfile.write(body)

@contextmanager
def running_source(port=0):
    server=ThreadingHTTPServer(('0.0.0.0',port),Handler)
    server.source=Source()
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try: yield 'http://127.0.0.1:'+str(server.server_port),server.source
    finally: server.shutdown();server.server_close();thread.join()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description='启动本地资料来源')
    parser.add_argument('--port',type=int,default=8765)
    args=parser.parse_args()
    server=ThreadingHTTPServer(('0.0.0.0',args.port),Handler);server.source=Source()
    print('资料来源已启动，端口 '+str(args.port),flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.server_close()
