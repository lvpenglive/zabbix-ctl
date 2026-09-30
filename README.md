# zabbix-ctl

Zabbix 纳管后端。只提供接口，没有页面。值班人员从 MeridianOps 门户进入「监控纳管」，由 Gateway 鉴权后调用本服务。

## 接口

- `GET /health`
- `GET /api/instances`
- `GET /api/instances/:code/version`
- `GET /api/instances/:code/hosts`
- `GET /api/instances/:code/hosts/:hostId/problems`
- `GET /api/instances/:code/hosts/:hostId/metrics?hours=24`
- `POST /api/instances/:code/sync` 同步主机对照（按 IP 匹配配置项）
- `GET /api/instances/:code/governance` Proxy / 模板偏离 / 采集队列
- `POST /api/tasks` 创建启停/升级任务
- `GET /api/tasks/:id` 查看任务与每台结果

除 `/health` 外都要带 `Authorization: Bearer <ZABBIX_CTL_SERVICE_TOKEN>`。

## 环境变量

| 变量 | 作用 |
|------|------|
| `ZABBIX_CTL_SERVICE_TOKEN` | Gateway 调本服务 |
| `ZBX_TOKEN_{CODE}` | Zabbix API 令牌 |
| `MERIDIANOPS_JOB_TOKEN` | 调 Gateway 作业的 API Token（`mk-` 前缀） |
| `MERIDIANOPS_DB_URL` | 覆盖数据库连接 |

`MERIDIANOPS_ZABBIX_CTL_TOKEN`（Gateway 侧）必须与 `ZABBIX_CTL_SERVICE_TOKEN` 相同。

## 运行

```bash
copy zabbix-ctl.toml.example zabbix-ctl.toml
cargo run
```

默认监听 `127.0.0.1:8090`。数据库连得上时会自动迁移并启动任务工人；连不上时只读接口仍可用。

## 发布包部署（Linux / 麒麟）

GitHub Actions 产物里已含 `start.sh` 与 `config/env.sh.example`：

```bash
cp config/zabbix-ctl.toml.example zabbix-ctl.toml   # 改库、Zabbix
cp config/env.sh.example config/env.sh && chmod 600 config/env.sh
# 编辑 env.sh 填令牌
./start.sh
# 或: nohup ./start.sh > zabbix-ctl.log 2>&1 &
```
