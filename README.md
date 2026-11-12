# ots-postgresql

[![CI](https://github.com/pandada8/ots-postgresql/actions/workflows/ci.yml/badge.svg)](https://github.com/pandada8/ots-postgresql/actions/workflows/ci.yml)

`ots-postgresql` 是一个暴露 **阿里云表格存储（Tablestore / OTS）协议** 的服务，把请求代理到一个 **PostgreSQL** 数据库。它用 PostgreSQL 模拟 OTS 的数据模型与读写接口，方便在 **开发、联调、单元测试** 等场景下，让使用官方 Tablestore SDK 的应用无需连接真实 OTS 实例即可运行。

> ⚠️ 本项目仅用于开发与测试，**不是** 生产级 OTS 替代品。它在一致性、性能、容量与功能完整性上都与真实表格存储不同，请勿用于生产环境。

## 功能特性

通过 HTTP + Protobuf + PlainBuffer 暴露以下 OTS 接口（与官方 SDK 兼容）：

| 分类 | 接口 |
| --- | --- |
| 表管理 | `CreateTable`、`ListTable`、`DescribeTable`、`UpdateTable`、`DeleteTable` |
| 单行读写 | `PutRow`、`GetRow`、`UpdateRow`、`DeleteRow` |
| 批量读写 | `BatchGetRow`、`BatchWriteRow`（支持 `IsAtomic` 原子批量） |
| 范围读取 | `GetRange`（正序 / 逆序、分页断点） |
| 二级索引 | `CreateIndex`、`DeleteIndex`（全局 / 本地索引，`IncludeBaseData` 存量回填） |
| 多元索引 | `CreateSearchIndex`、`ListSearchIndex`、`DescribeSearchIndex`、`DeleteSearchIndex`、`Search` |

支持的数据模型与能力：

- **主键**：`INTEGER` / `STRING` / `BINARY`，1–4 列主键；支持 **自增主键**（`AUTO_INCREMENT`，仅 `INTEGER`，映射为 PostgreSQL `GENERATED ALWAYS AS IDENTITY`）。
- **属性列类型**：`INTEGER`、`DOUBLE`、`BOOLEAN`、`STRING`、`BINARY`。
- **多版本**：`MaxVersions` 版本上限、按时间戳读取（`TimeRange` 区间 / 指定时间戳）。
- **TTL**：`TimeToLive` 过期版本在读取时过滤。
- **条件更新**：行存在性期望（`IGNORE` / `EXPECT_EXIST` / `EXPECT_NOT_EXIST`）+ 列条件（`SingleColumnCondition`、`CompositeColumnValueFilter`）。
- **原子计数器**：`UpdateRow` 的 `IncrementColumn` 与 `RT_AFTER_MODIFY` 返回修改后的值。
- **范围读取**：`FORWARD` / `BACKWARD`、`INF_MIN` / `INF_MAX` 边界、`limit` 与 `NextStartPrimaryKey` 分页断点。范围遵循 **起始主键包含、结束主键不包含**（与官方一致，逆序时起始主键大于结束主键）。
- **二级索引**：全局索引（Global）与本地索引（Local）。索引主键列须为主表主键或预定义列（`INTEGER` / `STRING` / `BINARY`），本地索引首列须与主表首主键一致；支持覆盖列（covering columns）。写入主表时在事务内同步维护索引，`CreateIndex` 可选 `IncludeBaseData` 回填存量数据；以索引名通过 `GetRange` / `GetRow` / `BatchGetRow` 读取，`DescribeTable` 返回索引元信息。
- **多元索引 SearchIndex**：以 PostgreSQL 表 + SQL 模拟多元索引的查询与统计能力：
  - **字段类型**：`LONG` / `DOUBLE` / `BOOLEAN` / `KEYWORD` / `TEXT` / `DATE`（`DATE` 支持自定义 `date_formats`，按声明格式解析为毫秒时间戳）。索引字段既可以是属性列，**也可以是主键列**（与真实 OTS 一致）。
  - **查询**：`MatchAllQuery`、`TermQuery`、`TermsQuery`、`RangeQuery`、`BoolQuery`（must / must_not / filter / should + `minimum_should_match`）、`ExistsQuery`、`PrefixQuery`、`WildcardQuery`、`MatchQuery`、`ConstScoreQuery`。
  - **聚合**：`sum` / `count` / `avg` / `min` / `max` / `distinct_count`。
  - **分组**：`GroupByField`（支持子聚合，`GroupKeySort` / `RowCountSort` / `SubAggSort` 排序）。
  - **排序与分页**：`FieldSort` / `PrimaryKeySort`、建索引时的 `IndexSort`；`offset + limit` 与 **`next_token` 翻页**；`get_total_count` 返回命中总数。
  - **结果回查**：`columns_to_get` 按主键回主表读取属性列（含未建索引的列）。
  - **数据同步**：`CreateSearchIndex` 在同一事务内回填存量数据；之后主表的写入 / 更新 / 删除在事务内同步维护索引行。
  - **命名**：索引名在 **表内唯一**（不同表允许同名索引，与真实 OTS 一致）。
- **过滤器**：单列条件、组合条件、分页过滤器（pagination filter）、值转换（value transfer）。
- **宽行读**：`columns_to_get`、`start_column` / `end_column`。
- **可选签名认证**：兼容 SDK 的 V1（HMAC-SHA1）与 V4（HMAC-SHA256）签名；可校验 `AccessKeyId` / `AccessKeySecret` / 实例名。
- **健康检查**：`GET /healthz`。

## 限制

以下 OTS 能力 **不支持**（请求会返回 `OTSUnsupportedOperation` 等错误）：

- 通道服务 Stream / CDC（`EnableStream`）。
- 本地事务（Local Transaction / `TransactionId`）。
- SQL 查询、时序模型等。
- 预留吞吐量（`ReservedThroughput`）仅记录返回，不做实际限流。

多元索引为简化实现，与真实 OTS 的差异：

- `TEXT` 字段的 `MatchQuery` 为 **子串匹配**（`LIKE`），不做分词，也没有相关性打分；`KEYWORD` 上的 `MatchQuery` 等价于精确匹配。
- 不支持 `MatchPhraseQuery`、`NestedQuery`、地理位置查询、`FunctionScoreQuery` 等查询类型；分组仅支持 `GroupByField`（不支持 range / filter / geo 分组）；聚合不含 percentiles、top_rows 等。
- 查询直接翻译为 PostgreSQL SQL 执行，没有分布式搜索引擎；命中规模大时性能与真实 OTS 不同，`offset` 深翻页同样受 SQL 限制。

其它约束：

- `STRING` / `BINARY` 主键映射为 `BYTEA`，长度受 OTS 协议本身的主键上限约束。
- 属性列以 **JSONB** 存储于单列，单行大小受 PostgreSQL JSONB / 行大小限制。
- 表名需匹配 `[A-Za-z_][A-Za-z0-9_]{0,254}`。
- PostgreSQL 标识符上限为 **63 字符**；多元索引的二级键名为 schema 级唯一，故采用 `k_<sha1(表名)[:8]>_<序号>` 形式避免跨表冲突。
- 多元索引的 `KEYWORD` / `TEXT` 字段以 B-tree 索引加速，受 PostgreSQL **单条索引项约 2704 字节** 的上限约束：超长关键字 / 文本写入可能导致索引行超限报错（开发测试场景通常不会触及）。

## 快速开始

预构建镜像发布在 GHCR，`main` 分支每次推送由 CI 自动构建（`v*` tag 会额外打上 semver 标签）：

```bash
docker pull ghcr.io/pandada8/ots-postgresql:latest
```

### 方式一：Docker Compose（推荐）

一次性拉起 PostgreSQL 与本服务：

```bash
docker compose up --build
```

服务默认监听 `http://localhost:8080`，PostgreSQL 数据持久化在 named volume 中。默认未开启认证，SDK 可任意填写 AK/SK。

### 方式二：本地运行

先准备一个 PostgreSQL（例如用 Docker）：

```bash
docker run --rm -d --name ots-postgres-dev \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=ots \
  -p 127.0.0.1:54321:5432 \
  postgres:17-alpine
```

编译并运行：

```bash
go build -o ots-postgresql .
./ots-postgresql \
  -listen :8080 \
  -postgres-dsn 'postgres://postgres:test@127.0.0.1:54321/ots?sslmode=disable'
```

启动时会自动创建元数据表 `__ots_tables`；业务表在 `CreateTable` 时按需创建。

## 配置参数

命令行参数与环境变量等价（参数优先）：

| 参数 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `-listen` | `OTS_LISTEN` | `:8080` | HTTP 监听地址 |
| `-postgres-dsn` | `POSTGRES_DSN` | `postgres://postgres:postgres@127.0.0.1:5432/ots?sslmode=disable` | PostgreSQL DSN |
| `-table-prefix` | `OTS_TABLE_PREFIX` | 空 | PostgreSQL 表名前缀 |
| `-access-key-id` | `OTS_ACCESS_KEY_ID` | 空 | 要求客户端携带的 AccessKeyId（空则不校验） |
| `-access-key-secret` | `OTS_ACCESS_KEY_SECRET` | 空 | 用于验签的 AccessKeySecret |
| `-instance` | `OTS_INSTANCE_NAME` | 空 | 要求客户端携带的实例名（空则不校验） |
| `-log-level` | `OTS_LOG_LEVEL` | `info` | 日志级别：`debug` / `info` / `warn` / `error` |

只要 `-access-key-id`、`-access-key-secret`、`-instance` 任一非空即开启认证校验；三者全空时关闭认证，便于本地快速联调。

## SDK 示例（Go）

```go
package main

import (
	"fmt"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func main() {
	// endpoint 指向 ots-postgresql；未开启认证时 AK/SK 可任意填写。
	client := tablestore.NewClient("http://127.0.0.1:8080", "dev-instance", "ak", "sk")

	// 建表
	meta := new(tablestore.TableMeta)
	meta.TableName = "demo"
	meta.AddPrimaryKeyColumn("pk", tablestore.PrimaryKeyType_STRING)
	create := new(tablestore.CreateTableRequest)
	create.TableMeta = meta
	create.TableOption = &tablestore.TableOption{TimeToAlive: -1, MaxVersion: 1}
	create.ReservedThroughput = &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0}
	if _, err := client.CreateTable(create); err != nil {
		panic(err)
	}

	// 写入
	key := new(tablestore.PrimaryKey)
	key.AddPrimaryKeyColumn("pk", "row1")
	put := new(tablestore.PutRowChange)
	put.TableName = "demo"
	put.PrimaryKey = key
	put.AddColumn("name", "alice")
	put.AddColumn("age", int64(30))
	put.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	putReq := new(tablestore.PutRowRequest)
	putReq.PutRowChange = put
	if _, err := client.PutRow(putReq); err != nil {
		panic(err)
	}

	// 读取
	criteria := new(tablestore.SingleRowQueryCriteria)
	criteria.TableName = "demo"
	criteria.PrimaryKey = key
	criteria.MaxVersion = 1
	getReq := new(tablestore.GetRowRequest)
	getReq.SingleRowQueryCriteria = criteria
	resp, err := client.GetRow(getReq)
	if err != nil {
		panic(err)
	}
	for _, col := range resp.Columns {
		fmt.Printf("%s = %v\n", col.ColumnName, col.Value)
	}
}
```

## 存储映射

- **元数据表** `__ots_tables`：记录 OTS 表名、对应 PostgreSQL 表名、表结构（JSONB）与创建时间。
- **索引注册表** `__ots_indexes`：记录索引名、所属主表、对应 PostgreSQL 表名与索引元信息（JSONB）。每张二级索引对应一张独立的 PostgreSQL 索引表（索引主键列 + `__ots_attrs` JSONB 列），写入主表时同步增删改，索引属性列仅保留最新版本。
- **多元索引注册表** `__ots_search_indexes`：按 `(主表, 索引名)` 记录多元索引元信息（JSONB）与对应 PostgreSQL 表名。每张多元索引对应一张独立的 PostgreSQL 表：主键列使用保留名 `__ots_pk_0..N`（避免与同名索引字段冲突），每个索引字段一列（`LONG`→`BIGINT`、`DOUBLE`→`DOUBLE PRECISION`、`BOOLEAN`→`BOOLEAN`、`KEYWORD`→`VARCHAR(1024)`、`TEXT`→`TEXT`、`DATE`→`BIGINT` 毫秒时间戳）并建二级键；查询、聚合、分组翻译为 SQL 在该表上执行，命中后按主键回主表取属性列。
- **数据表**：每张 OTS 表对应一张 PostgreSQL 表，由主键列 + 一个 `__ots_attrs` JSONB 列组成；属性列的所有版本都序列化进该 JSONB 列。
  - `INTEGER` 主键 → `BIGINT`（自增主键使用 `GENERATED ALWAYS AS IDENTITY`，写入时省略该列并以 `RETURNING` 取回生成的值）。
  - `STRING` / `BINARY` 主键 → `BYTEA`。
- **表名规则**：加上前缀后若仍是合法标识符且 ≤ 63 字符，直接用作 PostgreSQL 表名；否则使用 `前缀 + t_ + sha1(表名)[:32]`，以兼容超长或含特殊字符的 OTS 表名。
- 行级写入在事务中执行（`SELECT ... FOR UPDATE` → 条件校验 → 修改），原子批量写入共用一个事务；upsert 使用 `INSERT ... ON CONFLICT ... DO UPDATE`。
- **并发与死锁自动重试**：PostgreSQL 没有 MySQL 的间隙锁，`EXPECT_NOT_EXIST` 的并发插入改由唯一约束冲突（SQLSTATE `23505`）触发整事务重试，重试时重新判定行存在性；事务遇到死锁 / 锁等待超时 / 序列化失败（SQLSTATE `40P01` / `55P03` / `40001`）时也自动重试（最多 20 次），保证高并发写入（如并发原子计数器）最终成功。

## 开发与测试

```bash
# 格式化 / 静态检查 / 构建
gofmt -l .
go vet ./...
go build ./...

# 单元测试（PlainBuffer 编解码，含与官方 SDK 的字节级交叉校验，无需 PostgreSQL）
go test ./internal/...

# 端到端测试（官方 SDK 打到进程内服务 + 真实 PostgreSQL）
# 需要先准备一个 PostgreSQL，例如：
docker run --rm -d --name ots-postgres-test \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=ots \
  -p 127.0.0.1:54321:5432 postgres:17-alpine

OTS_POSTGRES_TEST_DSN='postgres://postgres:test@127.0.0.1:54321/ots?sslmode=disable' go test ./e2e/ -v
```

未设置 `OTS_POSTGRES_TEST_DSN` 时，端到端测试会自动跳过。也可以使用 `make test` / `make test-e2e`（见 `Makefile`）。

## 注意事项

- 面向开发测试：不要依赖其性能、容量上限或强一致性语义与真实 OTS 一致。
- 属性列存于 JSONB，超宽行 / 超大值可能触及 PostgreSQL 限制。
- 多版本与 TTL 在读取时按配置过滤；版本裁剪发生在写入时（`MaxVersions`）。
- 二级索引为同步维护的简化实现：索引属性列仅存最新版本，索引读取不套用主表 TTL；索引名在全实例内唯一。
- 多元索引同样是同步维护的简化实现：索引行只保存建索引字段的最新值，不套用主表 TTL / 多版本；`TEXT` 匹配与统计语义见上文「限制」。
- 认证为可选项，生产暴露请务必开启签名校验或置于受控网络内。
