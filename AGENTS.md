# Repository Guidelines

## 项目结构与模块组织
核心库实现位于 `src/kvstore.cpp`，对外头文件位于 `include/kvstore.h`。测试和演示入口在 `tests/main.cpp`，其中包含基础持久化验证、演示模式和长时间压力测试。构建配置位于 `CMakeLists.txt`。编译产物默认输出到 `target/bin` 和 `target/lib`。

## 构建、测试与开发命令
在仓库根目录使用 CMake 配置并编译：

```bash
cmake -S . -B build
cmake --build build
```

构建后会生成 `target/bin/kv_test` 和 `target/lib/libkvstore.so`。

运行测试入口：

```bash
./target/bin/kv_test
./target/bin/kv_test demo
./target/bin/kv_test stress
```

默认模式用于常规验证。`demo` 用于检查多类型数据写入和重启后的持久化行为。`stress` 为长时间并发压测，只建议在排查并发问题时使用。

## 代码风格与命名规范
请遵循当前仓库的 C++17 风格：

- 使用 4 空格缩进，大括号与声明同行。
- 公有类型和方法使用 `PascalCase`，例如 `KVStore`、`Put`、`Compact`。
- 私有成员、辅助函数和局部变量使用 `snake_case`，例如 `db_file_path_`、`recover_from_wal`。
- 注释保持简短，仅在文件格式、并发逻辑或恢复流程不直观时补充说明。

当前仓库未配置格式化器或 lint 工具，因此提交前应主动保持与现有代码风格一致，包括命名、头文件顺序和空白风格。

## 测试指南
仓库当前未引入独立测试框架，`tests/main.cpp` 中的断言就是主要验证方式。新增测试时，优先扩展该文件，或在测试规模增长后补充新的 CMake 测试目标。测试函数命名应直接表达目的，例如 `simple_test`、`stress_test`，并明确写出对持久化或并发行为的假设。

## 提交与 Pull Request 规范
现有提交历史以简短、直接的主题行为主，常见前缀包括 `feat:` 和 `refactor:`。提交信息应聚焦单一改动，避免把不相关修改混在同一次提交中。提交 Pull Request 时建议包含：

- 变更了什么行为
- 如何验证本次修改
- 是否影响数据文件、WAL 或并发路径
- 只有在有助于说明问题时再附上日志或截图
