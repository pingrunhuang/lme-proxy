# 独立 reference_data 终端确认工具

`scripts/lme_reference_data.py` 专门打印 `TradableInstrumentID / ProductCode / ContractCode / 币种 / 到期日 / 日期标签 / merged / linked / 策略腿`，供人工核对后填写 `config/symbols.yaml`。

**不修改主 proxy，不加载 `.env`，不读写 symbols.yaml，不输出 CSV/JSON/YAML，不持久化，不生成 TAP symbol，不用价格猜品种。** ProductCode 保留官方原始代码，不擅自翻译为产品名称；ContractCode 也不按字符串前缀猜品种。如需查看所有已解码字段（含期权、结算、手数等），加 `--details`，只打印到终端。

## 版本与证据（先看）

- **只实现显式 `--spec-version 4.18`；拒绝4.13，不自动判断版本。** 当前主 proxy 仍是4.13，两者独立。
- `--spec-version` 是操作者对现场版本的确认，不是检测结果。报文没有被本工具用于自动证明版本；即使长度匹配，也不能证明4.13与4.18兼容。未确认现场版本前不要把运行结果当作权威映射。
- 本次未取得4.13官方完整字段表。官方历史PDF URL尝试返回403；这不证明文件不存在。现有附件《LME/CME交易所行情接入文档》p2只是引用Connectivity Guide，p7明确demo不支持115/615，不足以实现4.13定义解码。
- 本实现来自官方 [Client Interface Specification v4.18](https://www.lme.com/-/media/files/trading/systems/lmesource/lmesource-client-interface-specification-v4-18.pdf)：§3.3 包头，§3.4.2 Sequence Reset (100)，§3.6.1 Refresh Complete (203)，§4.1.2.1 pp33–37 Outright Definition (301)，§4.1.2.2 pp37–40 Strategy Definition (302)，§4.1.2.3 pp40–41 merged，§6.1–6.4 pp98–109序号与刷新，Appendix A pp147–148通道表。PDF SHA256：`6de2fe940070f5f58adbde8dde95785539c37638a1811ac98eb22c08bd6ae596`。
- [官方FAQ v1.0](https://www.lme.com/-/media/files/about/faqs/lmesource-faqs-v10.pdf) pp3–5、7–8解释滚动/单日期、编号关系和定义发布。SHA256：`c14e90383d7ee697447995a113f91b949ee429453add2e96205190ddeec841fc`。
- 电子市场115/615使用301（单腿）和302（策略）。303/304用于非电子参考数据，不是本工具支持范围；收到会报错退出，不能混接200/700。
- 300（138字节）、305（36字节）只校验长度并参与序号统计，不出现在目录表；300主要预告尚未首日交易的新合约，不要求每个301有300。
- **115/615实际组播IP和端口尚未核验**。必须根据现场环境的Connectivity Guide逐一提供，不要从112/612地址推算。下文尖括号内容都必须替换，不能直接执行原样占位符。

## 运行

在项目目录，用已安装项目依赖的Python3.12运行，无新增依赖。离线部署时可用先前包内的 `../.venv/bin/python`（取决于实际目录），不要用系统Python3.9，不需要 `uv sync`。

### 推荐先做PCAP确认（不加入组播）

抓包必须覆盖115与615，且包含**两个615 Refresh Complete之间的完整周期，以及足以覆盖结束水位的115消息或心跳**。只抓几十个包通常不够。支持现有transport的classic PCAP（Ethernet/raw IPv4 UDP），不支持PCAPNG、IP分片；不按录包时间休眠。

```bash
cd /path/to/lme_proxy
.venv/bin/python scripts/lme_reference_data.py \
  --spec-version 4.18 --pcap /path/to/reference.pcap \
  --incremental '<已核验115-A组播IP>:<端口>' \
  --incremental '<已核验115-B组播IP>:<端口>' \
  --snapshot '<已核验615-A组播IP>:<端口>' \
  --snapshot '<已核验615-B组播IP>:<端口>'
```

A/B参数可重复，也可只有实际接入的一条线。只处理指定目的IP/端口的数据，忽略其他目的组。所有输入仍需来自可信抓包：本工具不验证发包方身份。

### 可选Linux live

**先停止同机主 proxy 和其他同端口监听者，再单独运行。** 本工具复用现有“每端口一个socket + IP_PKTINFO分流”的receiver，不改主程序；未验证不同进程间同时bind共享端口的行为，因此不宣称可并跑。`--standalone` 是操作者确认已隔离运行。

```bash
.venv/bin/python scripts/lme_reference_data.py \
  --spec-version 4.18 --live --standalone \
  --interface-ip '<实际接收LME组播的网卡IPv4>' \
  --incremental '<已核验115组播IP>:<端口>' \
  --snapshot '<已核验615组播IP>:<端口>' \
  --timeout 120 --details
```

`ens6f0 / 10.18.45.1`只是此前看到的候选行情网卡，仍需现场确认。防火墙、rp_filter、路由和接入权限由现场确认；脚本不会修改系统设置。Mac上请使用PCAP模式，live依赖Linux IP_PKTINFO。

## 看结果

**下面完全是合成示例，不是真实LME合约，不要填进symbols.yaml。** XX/SYNTHETIC是测试占位内容，编号42也不是现场合约证据。

```text
Status: SYNCED (as-of capture, not proof of live freshness)
TID  TYPE      PRODUCT CONTRACT  VENUE CCY EXPIRY   PROMPT LABEL PRICE MERGED LINKED LEGS REVIEW
42   outright  XX      SYNTHETIC EL    USD 20270225 R      3M    -     -      -      -    manual symbol review
```

实际用制表符分列。`PROMPT=R`是滚动日期，`S`是单一固定日期；标签3M和实际ExpiryDate要同时看。`MERGED`可能指向自己，或指向真正收到行情的另一个编号，不能当两个独立盘口计数。`LINKED`是TaS/TaR等关联，不等于同义合约。策略显示策略腿编号，字段详情保留买卖方向和比例；缺少腿/merged目标、merged环会在REVIEW列提示。

`SYNCED`**只表示本次615全周期与115序号水位覆盖成立**，不证明交易所身份、真实协议版本、业务日或当前新鲜度，也不意味着已得到无歧义TAP symbol。PCAP可能很旧。完整但空的目录也只能说明该次图像为空，不应据此推断某编号是什么。

工具在第一次同步成功时停止，展示该时点，不监控后续115变化；PCAP后面的包不会再处理。退出码0=此次同步成功；2=参数/输入错误、缺包、超时、中断或未同步。未同步时可显示已暂存记录，但全表醒目标为 `PARTIAL / UNCONFIRMED`，不得当成确认表使用。

要人工填写 `LME:F:NI:3M` 等名称，必须核对完整合约身份：ProductCode、ContractCode、ContractType、币种、venue、PriceCode、滚动/固定日期及官方定义。工具不把同金属所有到期日都命名为3M，不按“今天是三个月后”手算日期；同一滚动编号的实际日期也可能跨日变化。`122864`及此前其他现场数字ID仍未由真实定义验证。

## 完整性、边界和测试

- 同时收115并缓存，615先丢弃至首个203边界，再暂存下一个完整周期；结束203的LastSeqNum是115水位，不是615序号。丢弃水位之前的115效果，连续应用水位之后的增量，最后一次性激活目录。
- 115/615各自序号空间，A/B按消息序号去重；有限缓存支持乱序补齐。615缺口不越过；115水位后有缺口也不确认。心跳用于证明115当前位置，不替代丢失消息。
- Reset清空双方未完成状态，并拒绝早于reset发送时间的旧包；不复用112/612或614状态，不实现DR订阅/RTS补包，不自动修复永久缺口。重新抓取/运行即可。
- 默认60秒，最多3600秒；默认总缓存100000消息/32MiB、50000条定义，可用 `--max-messages / --max-bytes / --max-records` 调整。达到上限或遇到未知消息、布局不符、冲突重复、回退水位，保守失败，不冒充成功。
- 检查消息长度、策略腿计数、ASCII控制字符；整包校验后才改变状态。没有删除消息的猜测实现，没有缓存持久化/跨日恢复，也不把缺失记录猜成有效映射。
- `tests/test_reference_data.py`全部使用**按官方字段表构造的合成fixture/PCAP**，不是交易所抓包。本工具未在真实组播或真实115/615录包验收；部署前仍需核对现场版本与真实样本。

```bash
.venv/bin/python -m pytest tests/test_reference_data.py -q
.venv/bin/python -m pytest -q
```
