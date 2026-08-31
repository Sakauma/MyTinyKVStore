# 显式事务与 OCC 边界

## 接口

```cpp
auto tx = store.BeginTransaction();
auto value = tx.Get(1);
tx.Put(1, new_value);
tx.Delete(std::string("old"));
tx.Commit();
```

`KVTransaction`：

- 不可复制、可以移动。
- 支持 int、string 和 binary `Get/Put/Delete`。
- 支持 read-your-writes；同一 key 的最新暂存 mutation 优先于已提交状态。
- `Rollback()` 丢弃全部暂存 mutation，且不写 journal。
- 活动事务析构时自动 rollback。
- `Commit()` 后事务失效，无论提交成功还是抛出冲突。
- 暂不支持事务内 `Scan`。

## Shard-version OCC

事务第一次访问某个 shard 时记录该 shard 的版本：

- `Get` 记录读取时版本和 value。
- 首次 `Put/Delete` 也会观察目标 shard 版本，因此 blind write 仍参与冲突检测。
- 后续访问同一 shard 必须基于同一记录版本。

提交请求进入 ordered coordinator 后，会在写 journal 之前重新验证全部记录版本。版本不匹配时：

- 原子失败并抛出 `KVStoreConflictError`。
- 不分配 LSN。
- 不写 journal。
- 不发布任何 mutation。

验证成功的事务被编码为一个 transaction frame。涉及多个 shard 时按 shard ID 排序加锁，全部 mutation 应用后再释放。

## 隔离级别

该模型提供可串行化 OCC：事务提交点由 coordinator 的全局顺序定义，读集和写集涉及的每个 shard 都在提交前重新验证。

冲突粒度是 shard，不是 key。因此两个事务即使访问同一 shard 上不同 key，也可能发生保守冲突。默认 256 shard 用于在冲突精度、锁数量和索引管理之间取平衡。

### Lost update

两个事务读取同一 shard 版本后都更新该 shard，先提交者递增版本；后提交者验证失败，不会覆盖先提交者。

### Write skew

如果事务读取多个 shard 再写其中一部分，它仍记录全部读 shard。任一相关事务先改变其中一个版本，后提交事务就会失败，因此典型跨 shard write skew 不会同时提交。

### 只读事务

只读事务不会写 frame，但仍必须显式 `Commit()`。Coordinator 验证它观察过的全部 shard；如果期间发生变化，抛出 `KVStoreConflictError`。

## 与隐式事务的关系

- 单条 `Put/Delete` 是一个没有外部读集的隐式事务。
- `WriteBatch` 是一个包含多条 mutation 的隐式事务。
- 显式事务、隐式事务和 `Flush()` 共用同一请求序号和 ordered coordinator。
- 同一 group 中前面的隐式写会更新虚拟 shard version，因此后面的显式事务可以在落盘前直接判定冲突。

## 重试规则

`KVStoreConflictError` 是可重试的业务并发结果；调用方必须新建事务并重新读取。`KVStoreError` 表示格式、I/O、生命周期或 sticky fatal 等运行错误，不能按普通 OCC 冲突盲目重试。

推荐模式：

```cpp
for (;;) {
    auto tx = store.BeginTransaction();
    try {
        auto current = tx.Get(1);
        tx.Put(1, compute_next(current));
        tx.Commit();
        break;
    } catch (const KVStoreConflictError&) {
        // backoff, then start a fresh transaction
    }
}
```

## 当前不支持

- 事务内范围扫描。
- 保存点和嵌套事务。
- 跨进程或分布式事务。
- 长期 MVCC snapshot。
- 用户可选隔离级别。

持久化返回条件见[一致性与持久化语义](semantics.md)。
