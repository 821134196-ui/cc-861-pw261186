# 本地事件总线与消费者协调器演示

一个**零第三方依赖**的本地演示系统，展示：

- 分区、offset 与检查点形式的本地事件总线；
- 消费者心跳、失联检测、Generation/Fence 与分区转交；
- 新旧消费者重叠执行时，旧消费者的迟到提交不能让检查点倒退；
- 独立的本地 HTTP 外部处理服务，使用幂等键去重；
- “外部处理成功，但确认响应丢失”后的重投与去重；
- 中文可视化控制台，可点击按钮**单步推进重平衡**。

## 启动

需要 Python 3.11+，无需安装依赖：

```bash
python3 run.py
```

或：

```bash
python3 -m eventbus_demo
```

默认地址：

- 控制台：<http://127.0.0.1:8080/>
- 幂等处理服务健康检查：<http://127.0.0.1:8091/health>

如端口被占用：

```bash
python3 run.py --port 18080 --processor-port 18091 --no-browser
```

## 页面操作路径

### 1. 确认初始状态

初始消费组 `order-processors` 使用 generation 1：

- `consumer-1` 负责 `orders-0`、`orders-1`；
- `consumer-2` 负责 `orders-2`；
- `orders-0` 检查点为 `1`，因此下一条待处理消息是 offset `2`。

### 2. 演示“成功但确认丢失 → 重投 → 去重”

1. 在“事件总线与检查点”面板选择 `orders-2`。
2. 点击 **“处理下一条但丢失确认”**。
3. 外部服务已经用幂等键 `idem:orders:2:0` 完成了一次真实副作用，但 HTTP 响应被模拟丢弃。
4. 因为没有收到成功结果，检查点仍为 `-1`，页面出现红色重投条。
5. 点击 **“用同一幂等键重投并提交”**。
6. 外部服务返回缓存结果并标记 `deduped=true`，不会再次扣款/履约；随后检查点推进到 `0`。

右侧“幂等处理服务账本”中的“真实副作用”始终只增加一次，“去重命中”增加。

### 3. 单步推进故障转移与重平衡

1. 点击 **“让 consumer-1 停止回应”**。这会停止它的心跳，但模拟旧进程仍可能在恢复后执行/提交。
2. 连续点击 **“单步推进：……”**，每点击一次只执行一个协调阶段：
   1. **检测心跳超时**：虚拟时钟前进，consumer-1 被标记为 dead；
   2. **递增 Generation / Fence**：generation `1 → 2`，旧实例仍持有 g1；
   3. **转交 p0、p1**：新所有者变为 consumer-2；
   4. **新旧实例重叠处理**：旧 C1 与新 C2 都处理 `orders-0@2`，相同幂等键使外部副作用只执行一次；C2 提交 offset 2；
   5. **新所有者推进检查点**：C2 处理 offset 3 并提交，p0 检查点变为 3；
   6. **旧实例延迟提交**：C1 尝试用 g1 提交 offset 2；
   7. **完成重平衡**。

第 6 步可以看到两层保护：

- **协调器层**：`fenced_stale_generation`，旧 generation 被拒绝；
- **存储层**：即使绕过协调器直接写 offset 2，也会得到 `checkpoint_regression_rejected`，检查点仍为 `3`。

页面底部和“检查点提交审计”表会展示完整证据。

## 关键不变量

### 检查点单调

检查点语义是“最后成功处理的 offset”，初始为 `-1`。存储层只接受：

```text
requested_offset >= current_checkpoint
```

更小的 offset 会被拒绝；相等 offset 视为幂等重复提交。

### Generation Fence

每次重平衡 generation 单调递增。提交必须同时满足：

- generation 等于当前 generation；
- 调用者是该分区当前所有者；
- 成员未被标记为 dead/fenced。

### At-least-once + 幂等处理

确认丢失时不推进检查点，因此同一事件会被重新投递。重投复用稳定幂等键：

```text
idem:<topic>:<partition>:<offset>
```

外部服务在自己的账本中保存该键；同键、同请求体的重试只返回首次结果，不重复执行副作用。同键但请求体不同会返回 `409 IdempotencyConflict`。

## 架构

```text
浏览器控制台
    │  /api/state, /api/rebalance/next, ...
    ▼
DemoEngine（确定性模拟与审计）
    ├── GroupCoordinator   成员、心跳、generation、所有权、fence
    ├── EventBus           分区日志、pending reads、单调检查点
    └── ProcessorClient ──HTTP──▶ IdempotentProcessor (:8091)
                                  └── 幂等键账本 + 副作用序列
```

主要文件：

- `eventbus_demo/bus.py`：本地分区事件总线和单调检查点存储；
- `eventbus_demo/coordinator.py`：消费组协调器与提交授权；
- `eventbus_demo/idempotency.py`：幂等 HTTP 服务、客户端、ACK 丢失模拟；
- `eventbus_demo/engine.py`：单步重平衡状态机和演示编排；
- `eventbus_demo/server.py`：控制台静态资源与 JSON API；
- `eventbus_demo/static/`：原生 HTML/CSS/JS 页面；
- `tests/test_demo.py`：核心不变量和 HTTP 流程测试。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖：

- 检查点倒退被拒绝；
- ACK 丢失后的真实 HTTP 重投与去重；
- 失联检测、generation 递增、分区转交；
- 新旧重叠后旧提交无法倒退检查点；
- 页面静态资源和完整 HTTP 单步重平衡流程。

## 本地 API 摘要

```text
GET  /api/state
POST /api/reset
POST /api/consumers/consumer-1/stop
POST /api/rebalance/next
POST /api/events
POST /api/deliveries/process-next
POST /api/deliveries/retry
```

示例：

```bash
curl -X POST http://127.0.0.1:8080/api/rebalance/next \
  -H 'Content-Type: application/json' -d '{}'
```

这是教学/本地模拟实现，不提供真实网络分区、持久化崩溃恢复或生产级消费协议；重点是把故障转移、fencing、检查点单调性和幂等重投的因果过程可视化、可单步验证。
