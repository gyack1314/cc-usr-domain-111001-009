# 演练信息事件订阅与失效游标补采资料源

本项目保存本地合成资料及对应的来源站行为，供资料接入开发使用。内容取自虚构场景，不代表真实活动、人员、设施或设备参数。来源服务只模拟公开读取与指定的表单行为，不包含资料采集端。

## 环境与运行

使用 Python 3.11 及以上版本，仅依赖标准库。来源只监听本机地址，端口可以通过参数调整。两个终端分别启动不同端口可以表示两个独立来源。

```sh
python -m source_site --port 8090
```

`source_site/source.py` 管理来源响应，`source_site/__main__.py` 是 HTTP 适配层，`sources/` 保存原文与接口约定。状态只存在来源进程内，重启来源会恢复初始样本。`/__control` 和 `/__calls` 属于本地运维入口，资料接入端的正常操作不使用它们。运维端可以通过 POST `/__control` 提供 JSON 对象改变 phase 或更新 state。phase 列表与业务接口见下文，细节见 `sources/contract.json`。

- `GET /push-sequence`：返回待发送到接收端的乱序推送序列，调用方负责POST
- `GET /events`：epoch与after，返回大于after的事件，连续进度由采集端维护
- `GET /snapshot`：epoch与page，固定barrier=10，分页快照
- `phases`：["base", "rotated", "snapshot-fail"]

用 Python 的 HTTP 客户端推进来源状态，例如：

```sh
python -c 'import urllib.request; r=urllib.request.Request("http://127.0.0.1:8090/__control", data=b"{\"phase\":\"base\"}", headers={"Content-Type":"application/json"}); print(urllib.request.urlopen(r).read().decode())'
```

## 开发检查

以下检查覆盖来源协议、异常响应和真实本地 HTTP 启动。检查不会要求访问公网，也不会写入业务状态目录。

```sh
python -m unittest discover -s tests -v
python -m compileall -q .
```

容器环境也可以复现相同检查：

```sh
docker build -t event-bridge-source .
docker run --rm --network none event-bridge-source
```

来源的HTTP异常是合成协议行为，便于资料接入端开发和诊断。样本可扩展；业务数据、原始HTML以及接口响应不应被当作真实部署凭据或真实业务事实。

## 变更采集桥接

`event_bridge/` 是资料接入端的桥接模块，把来源推送与主动补采落到同一个 SQLite 本地变更日志，供下游按游标读取。仅依赖标准库。

- 推送先持久化到收件箱再确认；同一 epoch 内 `event_id` 幂等，`offset` 唯一。
- 只有从连续确认 offset 开始的事件才按序应用；补采从连续进度开始，不使用已见最大 offset。
- 同一 `record_id` 只接受更高 `revision`；同 revision 不同内容进入冲突证据；`delete` 是带 revision 的墓碑，旧 revision 的 upsert 不会复活资料。
- 补采返回 410 时先完整保存绑定新 epoch 与 barrier 的分页快照，再原子切换视图并接 barrier 之后的事件；快照翻页失败不会替换当前可读视图。
- 下游日志使用独立的本地自增游标；`kind='change'` 是已应用的业务变更，`conflict`、`ignored`、`snapshot_switch`、`snapshot_failure`、`late_epoch` 是证据（`applied=False`）。

```python
from event_bridge import EventBridge, HttpSourceClient

source = HttpSourceClient("http://127.0.0.1:8090")
bridge = EventBridge(source, "state/bridge.sqlite3")
bridge.receive_push(events)      # 接收推送，返回连续确认进度
bridge.backfill()                # 主动补采；410 时自动做快照恢复
bridge.read_records()            # 当前资料（include_deleted=True 含墓碑）
page = bridge.read_log(0, 50)    # 下游日志分页；page["next_cursor"] 续读
bridge.close()
```

完整演示（缺口补采、410、快照失败重试、重启幂等）：

```sh
python -m event_bridge.demo
```
