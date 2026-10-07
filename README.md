# DIAN115 REMUX 同步插件（开发中）

**插件仅通过 DIAN115 的插件 Host API 工作。使用 DIAN 已配置的 Madow、115 和 Emby，不申请 Madow 开发者，不要求用户提供 Madow SDK 或 API Key。**

当前完成纯 Go 筛选与串行队列核心，尚无可安装包、真实服务适配器或已启用的定时任务。

## 工作目标

- 由 DIAN 提供 Madow REMUX 专区的全部 115 候选资源，每 6 小时检查一次。
- 跨所选 Emby 实例的全部电影、剧集库去重；已存在的剧目整体跳过。
- 同一 TMDB / 类型先选不重复的季集组合最多的分享，再选体积最大的分享。身份、集数或大小不明时暂缓该组。
- 当前需求不设单次或每日积分上限；积分消费通过 DIAN 完成并保留结果记录。
- 每次解锁前再查媒体库，串行转存和整理，批次结束后刷新 Emby，逐条确认入库。待入库剧目参与下轮去重。
- 定时运行使用 DIAN 插件任务，不依赖浏览器、Codex 或外置常驻代理。

## 实现边界

所有执行适配器只能使用宿主批准的 `host.call` 接口。Madow 查询、解锁交给 DIAN 的现有集成；115 凭据和 Emby 凭据留在 DIAN。插件不直接请求 madow.tv，不读取浏览器 Cookie，不提取宿主 SDK 或内部密钥。

`core/planner.go` 负责完整快照校验、跨库去重、候选排序。

`core/queue.go` 负责：

`queued → 再查 Emby → absent → DIAN 解锁 → DIAN 转存 → DIAN 整理 → 批次刷新 → 核验入库`

`core/runner.go` 要求先以原子 CAS 保存待执行命令，再调用宿主；恢复时核对已有任务结果。超时不视为失败，不直接重发付费操作。入库慢的条目不会阻止核验其他条目。

这些是内部接口，尚未绑定实际宿主网络请求。当前实现不能自行证明转存或入库成功。

## DIAN 接口接入情况

已在 DIAN 官方插件接口文档找到：

- Emby 实例、媒体库、分页媒体和季集覆盖查询；
- 聚合订阅创建与任务查询；
- 115 分享转存和异步任务查询；
- 整理任务与 Emby 刷新；
- Host Storage 持久化及 manifest 定时任务。

仍需确认 **DIAN 对插件开放的 REMUX 全目录枚举、分享候选详情、指定分享解锁接口**。已查阅的标准接口目录未列出这三项。不能用普通聚合订阅冒充“遍历全部 REMUX 并按集数/大小选定分享”。

官方还提供 `host_access: extended` 扩展访问，但存在受保护路由，安装时必须明确批准；它不是任意内部接口都可用的保证。只有核实实际路由、请求响应和插件调用权限后才能启用。宿主未开放的能力等待 DIAN 提供，不改回直接接入 Madow SDK。

参考：[DIAN 插件平台](https://github.com/madbrolab/dian115/tree/main/docs/plugin-platform)、[Host API 调用与扩展访问](https://github.com/madbrolab/dian115/blob/main/docs/plugin-platform/host-call-v2.md)。

## 验证

```sh
go test ./... -count=1
```

9 项核心测试覆盖排序、完整分页、身份歧义、串行任务、入库核验、持久化失败、超时恢复和待入库去重。没有进行生产转存联调。
