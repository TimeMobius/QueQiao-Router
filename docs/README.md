# QueQiao-Router 文档

本目录集中放置 QueQiao-Router 网关的**接口文档**与**数据库结构文档**。
它随代码一起提交（位于仓库 `docs/` 目录）；仓库 README 只保留基本功能介绍。

## 目录

| 文档 | 内容 |
| :--- | :--- |
| [API_LLM.md](./API_LLM.md) | 对外模型接口（OpenAI 兼容、Anthropic Messages、vLLM 扩展） |
| [API_RECORDS.md](./API_RECORDS.md) | 监控面板日志检索接口（`/dashboard/api/*`） |
| [API_ANALYSIS.md](./API_ANALYSIS.md) | 日志聚合分析接口（`/dashboard/api/analysis*`） |
| [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md) | SQLite 审计库表结构、索引、FTS、月度轮转与归档分片 |

## 约定

- 所有面板查询接口均为 **GET**、返回 JSON，**只读**：不写数据库、不产生 metrics、不写访问日志。
- 这些接口自身不参与网关转发，可安全频繁轮询。
- 网关默认**无认证**，暴露在网络上等于泄露审计数据（含完整明文 API Key），
  部署时务必为 `/dashboard` 增加认证或限制来源。
