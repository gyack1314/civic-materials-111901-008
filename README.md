# 多语种披露原件的 HTTP 缓存与来源身份采集

这是围绕城市低碳公开资料的本地研发项目。当前内容提供授权合成来源、资料和来源契约；资料收集端尚未开发。来源不会替收集端作身份、覆盖或交付判断。

## 运行来源

使用 Python 3.11 或 3.12，先执行 python -m pip install -r requirements.txt。

启动命令：python -m source.server --port 8765。浏览器或 HTTP 客户端访问 http://127.0.0.1:8765。接口和资料定义见 docs/source-contract.md。

## 检查来源

执行 python -m unittest discover -s tests -v。tests/test_source.py 验证本地来源的真实响应与素材可读性；tests/test_collector.py 验证采集端行为，全部以合成站点的实际请求为依据。

## 运行采集端

采集端在 collector/ 包中，仅依赖标准库。先启动来源，再（另开终端）：

```sh
# 资料员按所需语言与披露范围登记订阅
python -m collector --store collector_data subscribe --staff 小李 \
    --url http://127.0.0.1:8765/document --lang zh --encoding gzip
# 领取可读取原件（先按条件刷新，再核实内容，写交付件）
python -m collector --store collector_data claim --id <订阅ID>
# 刷新全部订阅；report 辨认哪些复用了来源确认、哪些重新下载
python -m collector --store collector_data refresh
python -m collector --store collector_data report
# 取消订阅只解除登记，不影响他人已领取的同内容原件
python -m collector --store collector_data unsubscribe --id <订阅ID>
# 核实登记与文件对应关系并恢复（含部分下载的联机身份核实）
python -m collector --store collector_data repair
# 站点授权按 origin 范围配置，跨站转址不携带凭据
python -m collector --store collector_data \
    --credential http://127.0.0.1:8765='Bearer <token>' claim --id <订阅ID>
```

采集端保存目录（默认 collector_data/）由产品独立管理，不随来源进程；其中 cas/ 是内容寻址的传输字节与解码正文，cache/ 是缓存登记，deliveries/ 是每次领取的交付件（含请求条件、取得时刻与转址链），journal/refresh.jsonl 是刷新记录。凭据只存在于命令行与内存，不写入任何登记。

## 容器

执行 docker build -t disclosure-http-cache .，然后 docker run --rm -p 8765:8765 disclosure-http-cache。测试可用 docker run --rm disclosure-http-cache python -m unittest discover -s tests -v。

source 提供来源服务，collector 是采集端，data 保存合成原始资料，tests 核对来源与采集端行为；docs 记录字段与交互契约。不要将运行日志、凭据或采集数据提交到版本库。
