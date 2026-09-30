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

除 `/health` 外都要带 `Authorization: Bearer <service_token>`。

## 配置（`zabbix-ctl.toml`）

令牌直接写在 toml 里即可：

| 字段 | 作用 |
|------|------|
| `[server].service_token` | Gateway 调本服务（与 `MERIDIANOPS_ZABBIX_CTL_TOKEN` 相同） |
| `[[zabbix]].api_token` | 该实例的 Zabbix API 令牌 |
| `[meridianops].job_token` | 调 Gateway 作业的 `mk-` 令牌（启停时需要） |

可选环境变量覆盖：`ZABBIX_CTL_SERVICE_TOKEN`、`ZBX_TOKEN_{CODE}`、`MERIDIANOPS_JOB_TOKEN`、`MERIDIANOPS_DB_URL`。

## 运行

```bash
cp zabbix-ctl.toml.example zabbix-ctl.toml
# 编辑填写 service_token / api_token / 数据库
cargo run
```

默认监听 `127.0.0.1:8090`。

## 发布包部署（Linux / 麒麟）

```bash
cp config/zabbix-ctl.toml.example zabbix-ctl.toml
# 编辑令牌与数据库
chmod 600 zabbix-ctl.toml
./start.sh
# 或: nohup ./start.sh > zabbix-ctl.log 2>&1 &
```
