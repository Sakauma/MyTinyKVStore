# 格式模块设计

## 所有权

[storage_format.h](../../src/internal/storage_format.h) 和 [storage_format.cpp](../../src/internal/storage_format.cpp) 拥有：

- 当前 magic、磁盘版本、结构和格式上限。
- CRC32C 与稳定 key hash。
- Superblock/index/frame/mutation/footer 编码。
- Checkpoint builder 和流式 runtime parser。
- 共享 `recover_file` / `inspect_file`。

[key_codec.h](../../src/internal/key_codec.h) 和 [key_codec.cpp](../../src/internal/key_codec.cpp) 只负责 int/string/binary key namespace 编码，与磁盘容器解析解耦。

## 设计规则

- 运行时不得复制 header checksum 或边界验证逻辑；需要生成格式结构时调用 `make_*` helper。
- 解析器不得信任 count/length 后直接 `reserve` 或分配。
- 完整 frame 的 header、payload、mutation 和 footer 都必须独立校验。
- 格式工具必须调用共享 parser，而不是只检查 magic。
- 活动解析器只接受当前磁盘版本。任何未来不兼容格式都必须先单独设计迁移边界，不能在主恢复路径中隐式改写。

公开格式说明见[文件格式](../file-format.md)。
