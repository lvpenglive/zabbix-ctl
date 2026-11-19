-- 安装任务：选包、安装目录、conf 模板后缀
ALTER TABLE zbx_tasks
    ADD COLUMN package_id VARCHAR(36) NULL AFTER proxy_group_id,
    ADD COLUMN install_path VARCHAR(512) NULL AFTER package_id,
    ADD COLUMN conf_suffix VARCHAR(64) NULL AFTER install_path;
