-- 安装任务：Agent ListenPort / Zabbix 接口端口覆盖
ALTER TABLE zbx_tasks
    ADD COLUMN agent_port VARCHAR(16) NULL AFTER server_active;
