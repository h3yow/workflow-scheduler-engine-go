# 本地工作流调度引擎（Go）

一个零外部依赖、可本地直接运行的工作流调度引擎。任务与运行状态保存在本地
JSON 快照文件中，进程重启后可恢复；手动 / 定时 / 事件三种触发方式共用同一条
触发链路，执行结果一致、可追溯，在失败、并发、取消、重启场景下行为确定。

## 目录结构

```
internal/workflow/   领域类型：定义、运行、任务状态、触发来源、原因码
internal/scheduler/  DAG 校验（环 / 缺失依赖）与依赖就绪计算
internal/store/      FileStore：互斥保护 + 原子重命名的 JSON 快照
internal/engine/     触发准入、并发策略、重试、取消/超时、重启恢复
internal/trigger/    定时触发器与事件触发器（都只调用 Engine.Trigger）
cmd/demo/            四个端到端场景的可运行演示
```

## 快速验证

```bash
# 全部单元测试（含并发 / 重试 / 恢复 / 取消），带竞态检测
go test -race -count=1 ./...

# 端到端演示（重试、并发跳过、队列串行、取消），状态写入 JSON 快照
go run ./cmd/demo -store /tmp/wf-demo.json
cat /tmp/wf-demo.json | head
```

测试日志中每一行都会打印 `source`（触发来源 manual/timer/event）、`run`
（运行标识）、任务标识与状态流转，例如：

```
wf=wf run=run-...-event task=b task.fail_retry running->waiting reason=task_failed attempt=1/3
```

这些流转同时写入快照中的 `audit` 数组，可按 run id 追溯。

## 工作流语义

### 定义与 DAG 校验

- 一个工作流由若干任务组成，任务用 `deps` 声明依赖；只有**所有依赖均已
  succeeded** 的任务才会被调度（Kahn 拓扑就绪）。
- 定义在注册时（`Engine.RegisterDefinition`，调度前）被校验，拒绝原因用
  不同的 `Reason` 编码，可程序化区分：
  - `dag_missing_dependency`：依赖指向不存在的任务；
  - `dag_cycle`：存在环（含自依赖、重复任务 id）。
- 非法定义不会产生任何运行。

### 运行与任务状态

任务状态机（终态永不回退）：

```
pending -> running -> succeeded
                   \-> waiting -> pending（下一次重试）-> ...
                   \-> failed（重试额度耗尽，终态）
pending/waiting/running -> canceled（取消/超时）
pending -----------------> skipped（上游 failed，被阻断）
```

运行状态：`queued -> running -> succeeded | failed | canceled | timedout`。

### 任务不重复执行

- 每个运行由唯一 goroutine 顺序驱动；任务只有在 `pending` 时才能经
  CAS 式的状态转换进入 `running`，且 **RUNNING 行先持久化、再调用执行器**。
- 同一工作流的并发触发受并发策略约束（见下），因此同一批任务不会被
  两个运行并发执行；已终态的任务拒绝任何状态回退。

## 失败与重试规则

- 每个任务配置 `max_attempts`（总尝试次数，含首次；默认 1，即不重试）与
  `backoff_milli`（固定退避毫秒数）。
- 任务执行失败后：仍有额度 -> 记一次 `attempts`、进入 `waiting`、退避后回到
  `pending` 再次执行；额度耗尽 -> `failed`（审计原因 `task_max_retries_exceeded`）。
- `attempts` 只增不减，重复触发 / 恢复都不会重复计数。
- 任务 `failed` 后，其**全部传递下游**被标记为 `skipped`（不会启动），
  运行以 `failed / task_failed` 结束，`message` 中给出失败任务、尝试次数与
  错误信息；不存在悬挂的中间态。
- 执行器 panic 被捕获并按失败处理（错误以 `task_panic` 前缀标识）。

## 重启恢复规则

启动时调用 `Engine.Recover`，读取快照中所有非终态运行：

1. 同一工作流若有多个未完成运行，重建内存队列，只恢复最早的一个，其余保持
   `queued`，前一个结束后自动接续。
2. 崩溃时停在 `running` 的任务：那次执行**确定没有回报结果**，其已持久化的
   `attempts` 计为一次消耗——
   - 仍有重试额度：回到 `pending` 立即再试一次（审计原因
     `recovered_interrupted_execution`）；
   - 额度已尽：直接 `failed`，下游 `skipped`，运行 `failed` 终止。
3. 崩溃时停在 `waiting`（退避中）的任务：回到 `pending` 立即重试。
4. 已终态（succeeded/failed/canceled/timedout）的运行不恢复、不重跑。

每次状态变更都重写完整快照（临时文件 + rename 原子替换），崩溃后要么看到
旧快照、要么看到新快照，不会读到半写文件。

## 并发与取消约定

### 三种触发来源一致

`manual` / `timer` / `event` 都只调用同一个 `Engine.Trigger`，因此并发策略、
运行超时、任务超时、取消行为完全一致；来源只体现在运行记录和审计日志里。
定时与事件触发器是薄封装（见 `internal/trigger`），通过 `eng.TriggerFunc()`
适配，不包含任何独立的调度语义。

### 并发策略（按工作流配置）

- `skip`（默认）：存在活动运行（含排队中）时，新触发**立即拒绝**，返回
  `Skipped=true`、原因 `already_running_skipped`；该触发对应的运行行被显式
  置为终态 `canceled`（携带上述原因），不留中间态。
- `queue`：触发按到达顺序排队，同一时刻只有一个运行在执行，前一个终态后
  下一个自动开始；各运行的任务互不重叠、各自恰好执行一次。

运行的生命周期独立于触发调用方的 `context`：调用方提前放弃等待只会摘除其
等待者，不会取消正在运行或已排队的运行。

### 取消与超时

- `Engine.Cancel(runID)` 取消指定运行；`run_timeout_milli` 提供运行级超时，
  `timeout_milli` 提供任务级超时。
- 取消 / 超时后：**不再启动任何新任务**；
  - 已在执行的任务通过 `ctx.Done()` 收到取消，返回 ctx 错误后记为
    `canceled`（可解释：审计中区分 `canceled` 与 `timeout`）；
  - 未启动 / 退避中的任务记为 `canceled`（detail 标明 never started /
    canceled during retry backoff）；
  - 取消前已成功的任务保持 `succeeded`，结果不被覆盖；
  - 取消后运行终态为 `canceled`，超时终态为 `timedout`，原因分别是
    `canceled` / `timeout`。
- 取消后不再重试：中断的本次执行直接计入已消耗尝试并终止运行。

## 原因码速查

| Reason | 含义 |
|---|---|
| `dag_missing_dependency` | 依赖的任务不存在（调度前拒绝） |
| `dag_cycle` | 依赖环 / 自依赖 / 重复任务 id（调度前拒绝） |
| `already_running_skipped` | skip 策略下活动运行存在，触发被拒绝 |
| `task_failed` | 任务失败导致运行失败（或取消时仍有跳过任务） |
| `task_max_retries_exceeded` | 任务尝试次数耗尽 |
| `task_panic` | 执行器 panic |
| `canceled` | 运行或任务被取消 |
| `timeout` | 运行级超时 |
| `recovered_interrupted_execution` | 重启恢复：中断执行计入尝试并重试 |

## 本地使用示例

```go
st, _ := store.NewFileStore("state.json")
eng := engine.New(st)
_ = eng.Recover(ctx)

def := workflow.Definition{
    ID: "etl", Concurrency: workflow.ConcurrencyQueue, RunTimeoutMilli: 5000,
    Tasks: []workflow.TaskDef{
        {ID: "extract", MaxAttempts: 1},
        {ID: "transform", Deps: []string{"extract"}, MaxAttempts: 3, BackoffMilli: 100},
        {ID: "load", Deps: []string{"transform"}, MaxAttempts: 1},
    },
}
_ = eng.RegisterDefinition(ctx, def)
eng.RegisterExecutor("etl", "transform", func(ctx context.Context, wf, task string, attempt int) error {
    // attempt 从 1 开始；需要及时响应 ctx.Done()
    return nil
})

res, err := eng.Trigger(ctx, "etl", workflow.TriggerManual) // 或 timer / event
```
