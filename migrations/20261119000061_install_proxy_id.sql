-- 安装任务可选挂到 Zabbix Proxy（空表示直连 Server）。
ALTER TABLE zbx_tasks
    ADD COLUMN proxy_id VARCHAR(64) NULL AFTER server_active;
