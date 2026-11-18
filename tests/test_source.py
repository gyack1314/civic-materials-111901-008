"""本地来源契约检查，不包含尚未实现的采集逻辑。"""
import gzip
import http.client
import json
from pathlib import Path
import sys
import unittest
from urllib.parse import urlsplit
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from source.server import running_source

class SourceContract(unittest.TestCase):
    def request(self,url,payload=None,headers=None):
        parts=urlsplit(url);conn=http.client.HTTPConnection(parts.hostname,parts.port,timeout=10)
        body=json.dumps(payload).encode() if payload is not None else None
        conn.request('POST' if payload is not None else 'GET',parts.path+('?' + parts.query if parts.query else ''),body,headers or {})
        response=conn.getresponse();result=(response.status,dict(response.getheaders()),response.read());conn.close();return result
    def get(self,url,headers=None):return self.request(url,headers=headers)
    def post(self,url,payload):
        status,headers,body=self.request(url,payload)
        self.assertLess(status,300)
        return json.loads(body)
    def test_observable_source(self):
        with running_source() as (base,source):
            self.assertEqual(self.get(base+'/health')[0],200)
            self.post(base+'/__admin/advance',{'seconds':2})
            self.assertEqual(json.loads(self.get(base+'/health')[2])['time'],2)
            self.assertGreaterEqual(len(json.loads(self.get(base+'/__admin/log')[2])),3)

    def test_variant_and_weak_range(self):
        with running_source() as (base,s):
            a=self.get(base+'/document',{'Accept-Language':'zh','Accept-Encoding':'gzip'})
            self.assertEqual(gzip.decompress(a[2]).decode(),'合成披露正文')
            identity=self.get(base+'/document',{'Accept-Language':'zh','Accept-Encoding':'identity'})
            self.assertNotEqual(a[1]['ETag'],identity[1]['ETag'])
            self.assertEqual(self.get(base+'/document',{'If-None-Match':a[1]['ETag'],'Accept-Encoding':'gzip'})[0],304)
            self.post(base+'/__admin/advance',{'phase':1})
            w=self.get(base+'/document');self.assertTrue(w[1]['ETag'].startswith('W/'))
            self.assertEqual(self.get(base+'/document',{'Range':'bytes=2-','If-Range':w[1]['ETag']})[0],200)

if __name__=='__main__':unittest.main()
