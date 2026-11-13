-- 安装 Agent 任务：目标是配置项，创建时还没有 Zabbix hostid。
ALTER TABLE zbx_tasks
    ADD COLUMN server_addr VARCHAR(255) NULL AFTER target_version,
    ADD COLUMN server_active VARCHAR(255) NULL AFTER server_addr,
    ADD COLUMN group_ids TEXT NULL AFTER server_active,
    ADD COLUMN template_ids TEXT NULL AFTER group_ids;

ALTER TABLE zbx_task_hosts
    MODIFY host_id VARCHAR(64) NOT NULL;
