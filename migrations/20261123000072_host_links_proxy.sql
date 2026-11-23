-- 对照表记录监控通道，供启停时解析 SSH 跳板
ALTER TABLE zbx_host_links
    ADD COLUMN proxy_id VARCHAR(64) NULL COMMENT 'Zabbix proxyid' AFTER conf_suffix,
    ADD COLUMN proxy_group_id VARCHAR(64) NULL COMMENT 'Zabbix proxy_groupid' AFTER proxy_id;
