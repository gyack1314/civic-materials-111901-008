"""多语种披露原件采集端：HTTP 缓存、来源身份与交付管理。

围绕 docs/source-contract.md 的本地来源契约实现：

- 缓存键覆盖 URL、协商请求头与来源当时的 Vary，来源扩展 Vary 后旧条目
  只能回查，不能回答新请求；
- 传输字节（可能 gzip 编码）与解码正文分别登记摘要，验证器只对应它
  验证过的那个表示；
- 弱 ETag 不参与断点续传，部分文件只认相同强验证器；
- 每次领取生成独立交付件，记录请求条件、时刻与完整转址链；内容相同
  的交付共享内容寻址存储，取消订阅只解除本人的登记；
- 授权凭据按 origin 范围使用，重定向到范围外（如公共站 localhost）时
  不发送；
- 304 但本地无可用正文、登记与文件脱节、部分下载身份不符等异常都可
  通过 repair 核实并重新取得，绝不交付空件。
"""

from .service import Collector, CollectorError, EmptyDeliveryError

__all__ = ['Collector', 'CollectorError', 'EmptyDeliveryError']
