# lme-proxy

把 **LMEsource v4.13** 组播（或离线 PCAP）行情解码后，按 shenkong 已冻结的 ZeroMQ PUB/REP v1 协议推给 `multi-market-trading-engine` 策略。只做行情，没有报单。实现对照公开 C++ demo [futures_quote](https://github.com/zephyrize/futures_quote)（`src/lme.cpp`）以及 tap-proxy / fiu-proxy 的信封。

本项目**没有**完成交易所认证、生产网络验收或 RTS/TCP 补包，也不是生产级订单簿实现。请勿把 demo 级恢复逻辑直接用于生产。

## 架构

```text
LME UDP multicast (112/612/614, 可选 307/807)
        或 classic PCAP 回放
                |
                v
        lme-proxy 解码 / A/B 逐消息去重 / 612 刷新衔接
        PUB:5625    REP:5626
           |            |
           +------------+---- multi-market-trading-engine / 策略
```

解码线程（收包或 PCAP）只把事件放进发布队列。PUB、REP 各自只在自己的线程里使用 ZeroMQ socket，跨线程不共享 socket。

LME 组播是整通道加入，没有按合约的上游订阅。`subscribe_market_data` / `unsubscribe_market_data` 按 `client_id + strategy_id` 做引用计数，只控制**本地发布过滤**：第一个订阅者开始向该 symbol 发 tick，最后一个退出后停止。组播本身不会因此退组。

## 端口

| 服务 | PUB | REP |
| --- | ---: | ---: |
| ibkr-proxy | 5555 | 5556 |
| ctp-proxy | 5565 | 5566 |
| tap-proxy | 5575 | 5576 |
| atp-proxy | 5585 | 5586 |
| rh-proxy（RH_CN） | 5595 | 5596 |
| rh-proxy（RH_GLOBAL） | 5605 | 5606 |
| fiu-proxy | 5615 | 5616 |
| **lme-proxy** | **5625** | **5626** |

`ping.data.gateway` 为 `LME`，`ping.data.service` 为 `lme-proxy`。

`ready` 为 `true` 的条件：

- 传输已就绪：live 模式已加入组播，或 PCAP 回放已经开始；并且
- 解码器可用：若配置了 112，则至少完成一次 612 刷新同步且当前没有未补齐的序号缺口 / 无效盘口；若只有 307，则 LBMA 刷新已同步；若只有 614，传输启动即可（614 是独立成交回放流，不恢复 112 盘口）。

出现缺口、刷新无法衔接或盘口被标为无效时，`ready` 回到 `false`，并在 `status.LME` / `errors.LME` 发出 `FEED_GAP` 或 `BOOK_INVALID`。此时不会再把先前盘口当作新鲜行情发布。

## 配置

```bash
cp .env.example .env
# 按需填写 ZMQ 端口、LME_INTERFACE_IP、LME_SYMBOL_MAP、LME_SYMBOLS
# 排查收包/同步问题时把 LOG_LEVEL 改成 DEBUG
```

日志级别由 `.env` 里的 `LOG_LEVEL` 控制（`TRACE` / `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL`，默认 `INFO`）。`DEBUG` 会打出每条组播包的目的组、源地址、序号，以及 112 心跳和 612 刷新周期。即便保持 `INFO`，收包循环也会每 10 秒打一次计数（包括 0 包）和按目的组的拆分。

主配置是 `config/lme.yaml`，模板来自 demo 的 `lme-112-612-614-test.yaml`。目的组和 UDP 端口取自 *LME Systems Connectivity Guide v5.4* Production 表，**必须在测试环境用 tcpdump 核对**，不能当作可达证明。

- `network.interface_ip`：live 模式必填，填实际收到这些组播包的网卡 IPv4。也可用环境变量 `LME_INTERFACE_IP` 覆盖。`0.0.0.0` 或 `REPLACE_WITH_*` 不能用于 live。
- 先用 ASM（不填 `source_ip`）。确认发包源后再给每个 endpoint 加 `source_ip`，改为 SSM。
- `config/symbols.yaml`：`TradableInstrumentID -> LME:F:NI:3M` 这类 TAP 风格合约。未映射时，topic 使用数字 ID，例如 `marketdata.LME.42`。
- 本代理不解码 115/615 合约定义，因此没有内置 symbol 映射。

Linux 测试机通常需要关闭源地址校验，并先抓包确认链路：

```bash
# /etc/sysctl.conf
net.ipv4.conf.all.rp_filter = 0
net.ipv4.conf.default.rp_filter = 0
net.ipv4.conf.<行情网卡>.rp_filter = 0
sudo sysctl -p

ip -4 addr show
ip -4 route
sudo tcpdump -ni <行情网卡> -c 5 'udp and dst host <目的组播IP> and dst port <端口>'
```

tcpdump 能见到包，只说明帧到了抓包点，不证明内核已经把包交给了 UDP socket。

## 运行

```bash
uv sync --group dev

# 检查配置（不加入组播）
uv run python src/main.py --check-config config/lme.yaml

# 离线 PCAP 回放（本机开发/回归用）
uv run python src/main.py --config config/lme.yaml --pcap /path/to/lme.pcap

# 现场组播（先填好 interface_ip）
LME_INTERFACE_IP=10.0.0.10 uv run python src/main.py --config config/lme.yaml
```

PCAP 是经典格式（Ethernet 或 raw IPv4 + UDP），尽快顺序回放，不按原间隔休眠，不支持 PCAPNG 或 IP 分片重组。回放结束后进程保持运行，ZeroMQ 仍可 `ping` / 订阅。

另开一个终端订阅并打印行情：

```bash
uv run python scripts/zmq_subscribe_market_data.py \
  --symbol 'LME:F:NI:3M' \
  --count 20 \
  --pretty
```

默认只打印带买一卖一的订单簿更新；要同时看成交等事件，加上 `--all-events`。不传 `--count` 时一直打印，Ctrl+C 退出并退订。远程 Proxy 用 `--host`、`--pub-port`、`--rep-port`。`--symbol` 也可填原始 `TradableInstrumentID`。

## 测试

不需要连接交易所。单元测试使用内存中的合成 LME 报文，以及测试里生成的微小 PCAP。

```bash
uv sync --group dev
uv run pytest -q
```

## ZeroMQ 协议

与 tap-proxy 冻结的 v1 信封相同。PUB 两个 frame：UTF-8 topic + UTF-8 JSON。`published_at` 是 Proxy 发布时的 Unix epoch **毫秒**。

Topics：

```text
marketdata.LME.<symbol>
status.LME
errors.LME
```

`<symbol>` 优先为已配置的 TAP 合约（例如 `LME:F:NI:3M`）；未映射时为 `TradableInstrumentID`。307 参考价使用 `LME:R:<commodity>:<session>:<currency>:<price_type>`。

PUB 信封：

```json
{
  "schema_version": 1,
  "event": "marketdata",
  "published_at": 1785232800123,
  "data": {
    "symbol": "LME:F:NI:3M",
    "exchange": "LME",
    "kind": "book",
    "last_price": null,
    "volume": null,
    "open_interest": null,
    "upper_limit": null,
    "lower_limit": null,
    "bid_price_1": 100.0,
    "bid_volume_1": 30,
    "ask_price_1": null,
    "ask_volume_1": null,
    "exchange_time": null,
    "exchange_timestamp": 0,
    "local_time": 1785232800120,
    "book_valid": true,
    "bids": [{"price": 100.0, "volume": 30, "orders": 2}],
    "asks": [],
    "raw": {
      "tradable_instrument_id": 42,
      "channel_id": 112,
      "feed_id": 112,
      "seq": 99,
      "msg_type": 322,
      "flags": ["BOOK_VALID", "EVENT_COMPLETE", "RECOVERED", "REPLAYED"]
    }
  }
}
```

`kind` 可能是 `book`、`trade`、`indicative`、`quote_request`、`trading_status`、`reference_price`。策略已经在读的字段（`last_price`、买卖价量、`exchange_timestamp`、`symbol`）保持在 `data` 顶层。LME 特有字段放在 `raw`，不破坏公共字段。

价格是 JSON number；无效/空值价格（官方 `INT64_MIN` / `valid=0`）发 `null`，**不会发 0**。`exchange_time` 仅在事件时间像 Unix 纳秒时格式化为 `YYYY-MM-DD HH:MM:SS.mmm`。

REP 请求/响应与 tap-proxy 相同，`schema_version` 为 1。已实现：`ping`、`status`、`subscribe_market_data`、`unsubscribe_market_data`。`place_order`、`cancel_order`、`get_account`、`get_positions`、`get_orders`、`get_trades` 返回 `not_implemented`（与 fiu-proxy 一样，本服务没有交易接口）。

```json
{"action":"ping","request_id":"health-1"}
```

```json
{
  "action": "subscribe_market_data",
  "request_id": "sub-1",
  "client_id": "engine-01",
  "strategy_id": "ni-arb",
  "symbols": ["LME:F:NI:3M"]
}
```

引擎侧示例：

```env
ENABLE_LME=true
LME_PROXY_HOST=127.0.0.1
LME_PROXY_PUB_PORT=5625
LME_PROXY_REP_PORT=5626
LME_PROXY_CLIENT_ID=multi-market-engine
LME_PROXY_STRATEGY_ID=default
```

ZeroMQ 没有身份认证。生产环境只用防火墙或私有网络暴露 5625/5626，尤其是 REP。

## 已实现的 LME 消息

对照 C++ demo 的 v4.13 解码器：

| 通道 | 角色 | 消息 | 行为 |
| --- | --- | --- | --- |
| 112 实时 + 612 刷新 | 同一 feed | 100/203 控制，322 聚合盘口（显式+隐含，最多 15 档），327 清盘，341 成交，311/312 交易状态，320 指示价，329 询价 | A/B 逐消息去重；612 两个 203 之间的刷新用 `LastSeqNum` 作为 112 水位 |
| 614 | 独立 snapshot feed | 100/203，341 | 独立序号与 203 周期；成交带 recovered 标志，可能与 112 或下一轮 614 重复，消费方需按合约 / MatchID / 取消标志去重 |
| 307 实时 + 807 刷新 | 可选 | 100/105/203，405 参考拍卖价 | 旧版 v4.13 LBMA 路径；**不能宣称与 v4.18+ 兼容**（405 已在后续版本移除） |

未实现、改 YAML 也无法启用：

- 111/611、113/613、115/615（无内置业务解码器；没有 115 时只能靠 `TradableInstrumentID`）
- LME RTS/TCP 补包
- LMEsource v4.18 及更新布局
- 订单录入

价格尺度按 v4.13：Core 行情为定点 `-6`。官方空值不会被改写成价格 0。

## 离线部署（RHEL 9 / 无公网）

目标机如果不能访问公网，也不要使用系统自带的 Python 3.9。请使用配套离线包 `lme_proxy-offline-rhel9-x86_64.tar.gz`（内置可搬迁的 CPython 3.12 与依赖 wheel）。解压后按包内 `README-OFFLINE.md` 执行 `./install.sh` 和 `./run.sh`。不要在目标机上运行 `uv sync`，也不要从 PyPI 安装。

## 限制

- Demo 级盘口与刷新衔接，不是认证后的生产实现。
- 乱序等待窗口约 128 条消息 / 2 ms（与 C++ demo 相同，时钟是 datagram 的纳秒时间）。
- 没有断线后的历史成交保证；状态不明、缺口或刷新不完整时，不要把旧盘口当新鲜数据。
- 组播地址必须向测试环境确认。
