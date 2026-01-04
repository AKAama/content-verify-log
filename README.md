# 数据迁移说明

## 功能说明

此工具用于将 MySQL 数据库 `tbl_verify_content` 表中的 `content` 字段（JSON 格式）解析后存储到 DuckDB 中。

## 安装依赖

在运行迁移之前，需要安装以下依赖：

```bash
go get github.com/marcboeker/go-duckdb
go get github.com/google/uuid
go mod tidy
```

## 配置

在 `etc/config.yaml` 中配置 DuckDB 路径：

```yaml
duckdb:
  dbPath: ./data/content.duckdb
```

## 使用方法

运行迁移命令：

```bash
go run main.go migrate --config ./etc/config.yaml --batch-size 100
```

或者编译后运行：

```bash
go build -o content-verify-log
./content-verify-log migrate --config ./etc/config.yaml --batch-size 100
```

## 数据字段说明

### 输入（MySQL - tbl_verify_content）
- `id`: 记录 ID
- `taskId`: 任务 ID
- `content`: JSON 字符串，包含：
    - `checkresultstr`: 原文
    - `checkresultjson`: 错误修正信息数组

### 输出（DuckDB - processed_content）
- `id`: UUID（自动生成）
- `original_text`: 原文（来自 checkresultstr）
- `modified_text`: 修改后的文章（根据 checkresultjson 修正）
- `pid`: 任务 ID（来自 taskId）

## 错误词替换逻辑

系统会根据 `checkresultjson` 中的错误信息，将原文中的错误词替换为正确词，生成修改后的文章。

