# 本地来源契约

GET /document 按 Accept-Language 选择中英文，按 Accept-Encoding 选择 gzip 或 identity。服务返回 Vary、Content-Language 与 ETag；传输内容和解码内容分别有其字节身份。/other 是另一个披露对象，/alias 是永久转址别名，/external 转向 localhost 相同端口；授权 origin 为 127.0.0.1，localhost 是公共站身份。
阶段 0 强验证器，阶段 1 内容修订且使用弱验证器，阶段 2 别名改向 /other 并扩展 Vary 到 X-Disclosure-Scope。支持 If-None-Match，匹配时 304 没有响应体；Range 是编码后表示字节，If-Range 只有相同强验证器才接受分段，否则返回完整 200。416 提供现有总字节数。请求日志包含请求头，生产程序不应在交付件暴露认证信息。

演练来源由 Python 标准库 HTTP 服务启动，默认端口 8765。GET /health 可查询来源时间与阶段；POST /__admin/advance 接收 seconds、phase 推进本地情形；GET /__admin/log 回查实际请求。这些控制只供操作员调试来源，不能替代产品从正常入口取得资料。来源进程重启会清空其演练状态；采集端自己的保存目录由产品独立管理。
