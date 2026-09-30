#!/usr/bin/env bash
# 把 zabbix-ctl release 二进制 + 示例配置打成 tar.gz
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

: "${PACKAGE_NAME:?PACKAGE_NAME is required}"
: "${PACKAGE_LABEL:?PACKAGE_LABEL is required}"

BIN="target/release/zabbix-ctl"
CFG_EXAMPLE="zabbix-ctl.toml.example"

if [[ ! -f "$BIN" ]]; then
  echo "missing binary: $BIN" >&2
  exit 1
fi
if [[ ! -f "$CFG_EXAMPLE" ]]; then
  echo "missing config example: $CFG_EXAMPLE" >&2
  exit 1
fi

STAGE="dist/${PACKAGE_NAME}"
rm -rf "$STAGE"
mkdir -p "$STAGE/bin" "$STAGE/config" "$STAGE/systemd"

cp "$BIN" "$STAGE/bin/"
chmod +x "$STAGE/bin/zabbix-ctl"
if command -v strip >/dev/null 2>&1; then
  strip "$STAGE/bin/zabbix-ctl" || true
fi
cp "$CFG_EXAMPLE" "$STAGE/config/zabbix-ctl.toml.example"
if [[ -f README.md ]]; then
  cp README.md "$STAGE/"
fi

cat > "$STAGE/systemd/zabbix-ctl.service" <<'EOF'
[Unit]
Description=zabbix-ctl (MeridianOps Zabbix control service)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/zabbix-ctl
ExecStart=/opt/zabbix-ctl/bin/zabbix-ctl
Restart=on-failure
RestartSec=3
# 敏感项用 Environment / EnvironmentFile，勿写入 toml
# Environment=ZABBIX_CTL_SERVICE_TOKEN=
# Environment=ZBX_TOKEN_DC1=
# Environment=MERIDIANOPS_JOB_TOKEN=
# EnvironmentFile=-/opt/zabbix-ctl/config/env

[Install]
WantedBy=multi-user.target
EOF

{
  echo "package=${PACKAGE_NAME}"
  echo "label=${PACKAGE_LABEL}"
  echo "git=$(git rev-parse HEAD 2>/dev/null || echo unknown)"
  echo "ref=${GITHUB_REF:-}"
  echo "built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$STAGE/BUILD.txt"

cat > "$STAGE/README-DEPLOY.txt" <<'EOF'
zabbix-ctl Linux / 麒麟包

内容
  bin/zabbix-ctl
  config/zabbix-ctl.toml.example
  systemd/zabbix-ctl.service

部署（银河麒麟 V10 SP3 示例）
  1. tar xzf 本包 -C /opt && mv /opt/zabbix-ctl-* /opt/zabbix-ctl
  2. cp config/zabbix-ctl.toml.example config/zabbix-ctl.toml 并改数据库、Zabbix API
  3. 在 WorkingDirectory 下放 zabbix-ctl.toml，或设 ZABBIX_CTL_CONFIG
  4. 导出 ZABBIX_CTL_SERVICE_TOKEN（与 Gateway MERIDIANOPS_ZABBIX_CTL_TOKEN 相同）
     以及 ZBX_TOKEN_<CODE>（CODE 大写）
  5. cp systemd/zabbix-ctl.service /etc/systemd/system/
     systemctl daemon-reload && systemctl enable --now zabbix-ctl
  6. curl http://127.0.0.1:8090/health

说明
  - 默认只监听 127.0.0.1:8090，仅给 Gateway 内网调用
  - 数据库连 MeridianOps 同一 MySQL（meridianops 库）
  - 麒麟包在 hxsoong/kylin:v10-sp3 容器内编译
EOF

mkdir -p dist
tar -C dist -czf "dist/${PACKAGE_NAME}.tar.gz" "${PACKAGE_NAME}"
ls -lh "dist/${PACKAGE_NAME}.tar.gz"
echo "staged ${PACKAGE_NAME} (${PACKAGE_LABEL})"
