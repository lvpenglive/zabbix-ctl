CREATE TABLE IF NOT EXISTS zbx_tasks (
    id               CHAR(36)     NOT NULL,
    instance_code    VARCHAR(64)  NOT NULL,
    action           VARCHAR(32)  NOT NULL,
    target_version   VARCHAR(64)  NULL,
    concurrency      INT          NOT NULL DEFAULT 10,
    maintenance_id   VARCHAR(64)  NULL,
    status           VARCHAR(32)  NOT NULL DEFAULT 'pending',
    requested_by     VARCHAR(128) NOT NULL DEFAULT '',
    error            TEXT         NULL,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at      DATETIME     NULL,
    PRIMARY KEY (id),
    KEY idx_zbx_tasks_status (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Agent 批量任务';

CREATE TABLE IF NOT EXISTS zbx_task_hosts (
    id              BIGINT       NOT NULL AUTO_INCREMENT,
    task_id         CHAR(36)     NOT NULL,
    host_id         VARCHAR(32)  NOT NULL,
    ci_id           CHAR(36)     NULL,
    job_run_id      BIGINT       NULL,
    version_before  VARCHAR(64)  NULL,
    version_after   VARCHAR(64)  NULL,
    status          VARCHAR(32)  NOT NULL DEFAULT 'pending',
    error           TEXT         NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_zbx_task_host (task_id, host_id),
    KEY idx_zbx_task_hosts_task (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='批量任务主机明细';
