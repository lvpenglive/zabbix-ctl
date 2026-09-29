-- 主机与配置项对照。令牌不入库。
CREATE TABLE IF NOT EXISTS zbx_host_links (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    instance_code   VARCHAR(64)  NOT NULL,
    host_id         VARCHAR(32)  NOT NULL,
    hostname        VARCHAR(255) NOT NULL DEFAULT '',
    ip              VARCHAR(64)  NOT NULL DEFAULT '',
    ci_id           CHAR(36)     NULL,
    agent_version   VARCHAR(64)  NULL,
    agent_available TINYINT      NULL,
    synced_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_zbx_host (instance_code, host_id),
    KEY idx_zbx_host_ci (ci_id),
    KEY idx_zbx_host_ip (ip)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Zabbix 主机与 CMDB 对照';
