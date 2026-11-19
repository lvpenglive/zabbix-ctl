-- 安装任务支持挂到 Zabbix 7 Proxy Group；多地址列表加长。
ALTER TABLE zbx_tasks
    ADD COLUMN proxy_group_id VARCHAR(64) NULL AFTER proxy_id;

ALTER TABLE zbx_tasks
    MODIFY server_addr VARCHAR(1024) NULL,
    MODIFY server_active VARCHAR(1024) NULL;
