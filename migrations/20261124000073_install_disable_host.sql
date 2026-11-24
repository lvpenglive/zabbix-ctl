-- 安装登记：agent.ping 验证通过后是否禁用主机（避免立刻告警）
ALTER TABLE zbx_tasks
    ADD COLUMN disable_host TINYINT NOT NULL DEFAULT 0
        COMMENT '1=验证通过后禁用 Zabbix 主机'
        AFTER conf_suffix;
