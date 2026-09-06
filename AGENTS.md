# Repository Guidelines

## 项目结构

- `include/kvstore.h`：公开 C++17 API、配置和指标。
- `src/kvstore.cpp`：公开 API 到内部实现的适配层。
- `src/internal/`：存储引擎、单文件格式、I/O、writer policy、cache 与 observability。
- `tests/unit/`：内部纯逻辑和格式单元测试。
- `tests/integration/`：持久化、恢复、事务、指标和并发集成测试。
- `tests/common/`：测试 runner、临时目录、CLI、benchmark 和 stress 公共代码。
- `scripts/`：CI、sanitizer、格式工具、benchmark、stress 与 qualification 入口。
- `docs/`：当前契约、格式、运行手册和内部设计；`archive/` 只保存历史索引及本地忽略归档。

## 构建与产物

在仓库根目录执行：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
(cd build && ctest --output-on-failure)
```

产物位于构建目录内：

- `build/target/bin/kv_test`
- `build/target/bin/kv_unit_test`
- `build/target/lib/libkvstore.so`

正式构建使用 `bash scripts/ci-build.sh`。ASan、UBSan 和 TSan 使用 `bash scripts/ci-sanitizers.sh`；直接配置 TSan CMake/CTest 时通过 `-DKVSTORE_TSAN_OPTIONS=...` 传递扩展选项，不能依赖配置后的进程环境覆盖 CTest 属性。长时间 stress、soak 和 qualification 只在任务明确要求时运行。所有正式性能与 qualification 工作负载必须使用原生 Linux ext4 临时目录，不能使用 `/mnt/*`、DrvFS、9p 或 fuseblk。

## 测试入口

CTest 将 unit 和 integration 按组注册，并为各组设置 label 与超时。直接运行测试时可列出或筛选用例：

```bash
./build/target/bin/kv_unit_test --list
./build/target/bin/kv_unit_test --filter "CRC32C"
./build/target/bin/kv_test --list
./build/target/bin/kv_test --list-groups
./build/target/bin/kv_test --group recovery-format
./build/target/bin/kv_test --filter "compaction"
```

筛选结果为空会返回非零状态。测试断言使用 `test_support::require` 抛出异常；runner 会记录失败并继续执行其余独立命名用例。需要保留数据库、临时文件和损坏现场时设置：

```bash
KVSTORE_KEEP_TEST_ARTIFACTS=1 ./build/target/bin/kv_test --filter "目标用例"
```

runner 会把保留目录的绝对路径写到标准错误。默认情况下测试目录位于系统临时目录并在用例结束时删除。

## 格式与运维工具

格式工具统一构建 Release `kv_test`，可用 `KVSTORE_BUILD_DIR` 指向仓库外构建目录：

```bash
bash scripts/inspect-format.sh <db_path>
bash scripts/verify-format.sh <db_path>
bash scripts/rewrite-format.sh <db_path>
```

`rewrite-format` 会修改数据库，执行前必须确认目标路径并保留所需备份。若任务使用 Python，只能调用 WSL 中已有虚拟环境，不要使用 Windows Python，也不要新建环境。

## 代码与测试风格

- 使用 4 空格缩进，大括号与声明同行，保持现有 include 顺序和空白风格。
- 公有类型和方法使用 `PascalCase`；私有成员、辅助函数和局部变量使用 `snake_case`。
- 只在格式、不变量、并发或恢复流程不直观时添加简短注释。
- 新测试优先加入对应 unit/integration registry，名称直接描述行为和假设。
- 改动恢复、WAL、compaction、事务或并发路径时，必须补覆盖成功路径和错误路径的回归，并运行相应 sanitizer 或有界并发验证。
- 不要用 benchmark 数字代替正确性测试；更新性能门槛必须保留工具链、提交、文件系统、命令和原始结果。

## Git 与协作

工作树可能包含其他协作者的修改。不要还原不属于当前任务的变更，也不要使用 `git reset --hard` 或 `git checkout --` 清理工作树。提交主题保持简短并聚焦单一改动，常用前缀为 `feat:`、`fix:`、`test:`、`docs:` 和 `chore:`。

Pull Request 或最终交付说明应包含：行为变化、验证命令与结果，以及是否影响数据格式、WAL、compaction、恢复或并发语义。正式 12 小时/1000 万对象 qualification 未实际完成时必须明确写为未达标。
