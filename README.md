# 本地工作流调度引擎（Go）

一个可在本地直接运行的工作流调度引擎。用本地 JSON 文件存储模拟任务与运行状态，
保证不同触发方式（手动 / 定时 / 事件）下的执行结果**一致、可追溯**，
并在**失败重试、并发触发、进程重启、取消/超时**等场景下保持状态可预期。

## 1. 快速开始

```bash
# 跑一个端到端演示（文件存储在临时目录，含 DAG、失败重试、并发去重）
go run . -dir ./.wfdata

# 运行全部单元测试（含真实子进程 SIGKILL 的重启恢复用例）
go test ./... -race -count=1
go test ./workflow/ -v            # 查看触发来源 / 运行标识 / 状态流转日志
```

演示与测试日志中每个状态流转都会打印：时间、**触发来源（manual/schedule/event）**、
**运行标识 runID/任务ID**、`from -> to` 状态、失败原因码与说明，形成可追溯的审计轨迹。

## 2. 核心抽象

| 类型 | 说明 |
| --- | --- |
| `WorkflowDef` | 工作流定义：一组 `TaskDef` 构成的 DAG，带并发策略与运行级超时 |
| `TaskDef` | 任务：ID、依赖列表、`RetryPolicy`、单次执行超时 |
| `Run` | 一次运行：唯一 `RunID`、触发来源、状态、终态原因、DedupKey |
| `TaskRun` | 运行中的任务：状态、`Attempt` 计数、输出、错误码/信息 |
| `Event` | 仅追加的状态流转事件，写入 `events.jsonl`，是审计与追溯的依据 |
| `Store` | 存储边界：`MemoryStore`（测试）与 `FileStore`（JSON 文件 + flock） |

所有状态变更都通过**比较并交换（CAS）**完成：
`CompareAndSwapTask(id, expect, mutate)` / `CompareAndSwapRun(...)`
只有当存储中的当前状态等于期望的前置状态时才会落库。这是"任务不重复执行、
状态不回退、并发触发只有一个胜出"的底层保证。

## 3. 工作流语义

- **依赖调度**：任务仅在其所有依赖均为 `SUCCEEDED` 后才进入可执行状态；
  调度器按依赖顺序推进，同一 DAG 内可并行的任务按 `WithMaxParallel`（默认 4）限流。
- **定义期校验，调度前拒绝**。注册工作流时 `ValidateDefinition` 返回
  `*ValidationError`，原因码可区分：
  - `CYCLE_DETECTED`：依赖图存在环（错误信息给出环上的节点）；
  - `MISSING_DEPENDENCY`：依赖指向不存在的任务；
  - `DUPLICATE_TASK` / `EMPTY_WORKFLOW` / `INVALID_DEFINITION`。
  校验失败的工作流不会创建任何运行记录。
- **失败阻断下游**：任务最终失败后，其所有下游任务标记为 `SKIPPED`，
  原因码 `UPSTREAM_FAILED`；运行终态为 `FAILED`，携带首个失败任务的错误码。

### 任务/运行状态机

```
任务: PENDING ──> RUNNING ──> SUCCEEDED
                    │           FAILED      （重试耗尽）
                    │           SKIPPED     （上游失败，从 PENDING 直接进入）
                    └─────────> CANCELED    （运行被取消/中断）

运行: PENDING ──> RUNNING ──> SUCCEEDED | FAILED | CANCELED | INTERRUPTED
```

终态不可逆，任何流转都以 CAS 保护，**状态只进不退**。

## 4. 失败重试规则

- 每个任务用 `RetryPolicy{MaxAttempts, InitialBackoff, MaxBackoff}` 配置：
  `MaxAttempts` 是**总尝试次数**（1 表示不重试）。
- 处理器返回 `*TaskError{Code, Msg}` 时，`Code` 作为稳定、可区分的失败原因码
  写入任务记录与运行终态（例如 `DENIED`、`TRANSIENT`、`TIMEOUT`）。
- 退避为指数退避：`InitialBackoff * 2^(attempt-1)`，上限 `MaxBackoff`。
- **计数只增不减**：下一次尝试由一次 `RUNNING -> RUNNING, Attempt+1` 的 CAS 领取，
  因此重试与恢复过程中既不会重复计数也不会回退。
- 超过上限：任务 `RUNNING -> FAILED`，原因 `RETRY_EXHAUSTED`（或处理器错误码），
  随后下游被跳过、运行失败。
- 单次尝试可用 `TaskDef.Timeout` 限时，超时记 `TIMEOUT` 并参与重试。

> 处理器应尽量**幂等**：进程崩溃可能发生在一次尝试已产生副作用但未写状态时，
> 恢复会对该任务重新发起一次调用（`Attempt` 不重置）。

## 5. 重启与恢复规则

`FileStore` 将定义/运行/任务写入 JSON（临时文件 + fsync + rename，原子替换），
事件追加到 `events.jsonl`（每条 fsync），并用 `flock` 保证同一目录只有一个引擎。
启动时 `NewEngine` 扫描所有非终态运行，按策略处理（`WithRecovery`）：

- **`resume`（默认）**：重新挂载非终态运行继续调度。
  崩溃时处于 `RUNNING` 的"存疑"任务会被重新调用一次，`Attempt` 在原值上 +1
  （例如崩溃时 `Attempt=1`，恢复后为 2，**绝不归零**）；`PENDING` 任务照常调度、
  已 `SUCCEEDED` 的任务不会重跑。
- **`terminate`**：把遗留运行明确置为 `INTERRUPTED`（原因 `RUN_INTERRUPTED`），
  所有未完成任务置为 `CANCELED` 并标注同一原因，避免中间态悬挂；
  此后可立即发起全新的运行。

恢复用例通过**真实子进程 + `SIGKILL`** 验证：子进程在任务运行中被杀，
父进程重新打开存储，断言磁盘上的中间态以及 resume / terminate 的最终结果。

## 6. 并发与取消约定

手动、定时（`Scheduler`）、事件（`EmitEvent`）三种触发**走同一个 `Engine.Trigger`
入口**，因此并发策略、超时、取消行为完全一致。每个工作流以"工作流名"为 DedupKey，
同一时刻只允许一个非终态运行。并发策略 `TriggerConcurrency`：

- `reject`（默认）：新触发被拒绝，返回包裹 `ErrRunActive` 的错误
  （原因语义对应 `ALREADY_TRIGGERED`），已存在的运行继续执行。
- `queue`：新触发排队等待当前运行结束后再启动；多个排队触发按唤醒依次串行执行，
  运行之间绝不重叠。
- `cancel`：先取消当前运行，待其完全收尾后再启动一个全新的替代运行。

"检查活动运行 → 依据策略决策 → 创建新运行"的关键区间持有引擎互斥锁，
配合任务领取的 CAS，保证**无论多少并发触发，一个任务在一次运行中只会执行一次、
一个工作流只有一个活动运行**。

取消与超时约定：

- 调用 `Cancel(runID, reason)` 后：
  - **不会再启动任何新任务**；尚未开始的 `PENDING` 任务置 `CANCELED`；
  - 已启动任务通过 `ctx` 收到取消信号，待其收尾后写入终态；
  - 若处理器在取消前后自行成功完成，其结果保留为 `SUCCEEDED`（输出不丢失、结果可解释），
    运行整体仍为 `CANCELED`；未成功完成的运行中任务置 `CANCELED`。
  - 运行终态为 `CANCELED`，并统计"已取消/已成功"任务数。
- 运行级 `WorkflowDef.Timeout` 到期等同于"带 `TIMEOUT` 原因的取消"：
  不再启动新任务、运行终态为 `FAILED/TIMEOUT`，与用户主动取消（`CANCELED`）可区分。
- 调度器在所有运行中任务都写出终态**之后**才计算运行终态，因此不会出现
  "运行已结束但任务仍悬空"的中间态。

## 7. 存储与本地验证

```
<dir>/
  workflows.json   # 工作流定义
  runs.json        # 运行记录（原子整文件写）
  tasks.json       # 任务运行记录（原子整文件写）
  events.jsonl     # 仅追加事件流（每行一个 JSON，每次写入 fsync）
  lock             # flock，单引擎互斥
```

```bash
# 演示
go run . -dir ./.wfdata

# 全量测试（竞态检测）
go test ./... -race -count=1

# 单独查看某类场景的状态流转日志
go test ./workflow/ -run 'TestRecovery|TestCancel|TestConcurrent|TestRetry' -v
```

## 8. 测试覆盖

| 场景 | 用例 | 关键断言 |
| --- | --- | --- |
| DAG 顺序 | `TestDAGSuccessOrder` | 依赖先后关系满足、运行成功 |
| 失败重试 | `TestRetryThenFailAndSkipDownstream` | 恰好执行 `MaxAttempts` 次、失败码可区分、下游 `SKIPPED/UPSTREAM_FAILED` |
| 重试恢复 | `TestRetrySucceedsEventually` | 尝试计数递增、最终成功且输出正确 |
| 最终重试成功 | 同上 | 状态不回退 |
| 并发触发 | `TestConcurrentTriggersDoNotDuplicate` | 20 个并发触发只产生 1 个运行、任务只执行 1 次、19 个被拒 |
| 并发策略 | `TestQueuePolicyRunsSequentialRuns` / `TestCancelPolicyReplacesActiveRun` | queue 串行不重叠；cancel 取消旧运行并替代 |
| 触发一致性 | `TestScheduledAndEventShareSemantics` | manual/schedule/event 在活动运行下以同一 `ErrRunActive` 被拒 |
| 定义校验 | `TestValidationCycleAndMissingDep` | `CYCLE_DETECTED` 与 `MISSING_DEPENDENCY` 可区分且不产生运行 |
| 取消 | `TestCancelSettlesRun` / `TestCancelKeepsHandlerSuccess` | 不启动新任务、未开始任务 `CANCELED`、自行成功的任务保留结果 |
| 超时 | `TestRunTimeoutFailsRun` | 运行 `FAILED/TIMEOUT` |
| 重启恢复 | `TestRecoveryResumeContinuesRun` / `TestRecoveryTerminateMarksInterrupted` | 真实子进程 SIGKILL；resume 计数不归零、terminate 明确 `INTERRUPTED` |

所有用例在 `-race` 下通过。
