# 恢复设计

## 入口

- [storage_format.cpp](../../src/internal/storage_format.cpp)：共享只读解析器和尾部修复。
- [storage_engine.cpp](../../src/internal/storage_engine.cpp)：把解析结果装入 shard index。
- [format_analysis.cpp](../../tests/common/format_analysis.cpp)：CLI inspect/verify 直接消费同一解析器。

## 顺序

1. `fstat` 文件大小。
2. 校验双 superblock，选择最高有效 generation。
3. 验证所有区域边界和 checkpoint CRC32C。
4. 流式读取 index/object 条目，建立 offset index。
5. 从 `journal_offset` 起解析严格递增 LSN frame。
6. 只应用 header/payload/mutation/footer 全部通过的事务。
7. 读写打开时截断可识别的不完整最终 frame。

## 不变量

- 任何磁盘长度都先验证再分配。
- Operation count 必须能由 payload 的最小 mutation 尺寸容纳。
- 完整损坏与不完整尾部严格区分；完整损坏不能跳过。
- `WriteBatch` 和显式事务按 frame 原子重放。
- Runtime 与 verifier 不维护两套校验规则。
- Journal 重放按完整物理 frame 为每条 mutation 分配 WAL charge，并重建与运行时一致的 live/obsolete accounting。
- 恢复只依赖主数据库路径，不扫描或采用遗留 `.compact.*` 文件；这些文件既不是提交标记，也不能覆盖主文件。

恢复路径只识别当前格式，不包含历史 v1/v2 数据迁移能力；历史数据迁移必须作为独立工具重新设计和验证，当前版本不能直接恢复这类历史数据。
