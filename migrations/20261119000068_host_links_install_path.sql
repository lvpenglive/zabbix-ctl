-- 主机对照保存安装目录与包信息，供启停/升级传 env
ALTER TABLE zbx_host_links
    ADD COLUMN install_path VARCHAR(512) NULL AFTER agent_available,
    ADD COLUMN package_id VARCHAR(36) NULL AFTER install_path,
    ADD COLUMN conf_suffix VARCHAR(64) NULL AFTER package_id;
