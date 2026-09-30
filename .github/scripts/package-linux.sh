#!/usr/bin/env bash
# 把 zabbix-ctl release 二进制 + 示例配置/启动脚本打成 tar.gz
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

: "${PACKAGE_NAME:?PACKAGE_NAME is required}"
: "${PACKAGE_LABEL:?PACKAGE_LABEL is required}"

BIN="target/release/zabbix-ctl"
CFG_EXAMPLE="zabbix-ctl.toml.example"
ENV_EXAMPLE="config/env.sh.example"
START_EXAMPLE="scripts/start.sh.example"

if [[ ! -f "$BIN" ]]; then
  echo "missing binary: $BIN" >&2
  exit 1
fi
if [[ ! -f "$CFG_EXAMPLE" ]]; then
  echo "missing config example: $CFG_EXAMPLE" >&2
  exit 1
fi
if [[ ! -f "$ENV_EXAMPLE" ]]; then
  echo "missing env example: $ENV_EXAMPLE" >&2
  exit 1
fi
if [[ ! -f "$START_EXAMPLE" ]]; then
  echo "missing start example: $START_EXAMPLE" >&2
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
cp "$ENV_EXAMPLE" "$STAGE/config/env.sh.example"
cp "$START_EXAMPLE" "$STAGE/start.sh"
chmod +x "$STAGE/start.sh"
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
ExecStart=/opt/zabbix-ctl/start.sh
Restart=on-failure
RestartSec=3

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
  start.sh                         前台启动（会 source config/env.sh）
  config/zabbix-ctl.toml.example
  config/env.sh.example            环境变量示例（令牌）
  systemd/zabbix-ctl.service       可选

不用 systemd（推荐先这样测）
  1. tar xzf 本包 -C /opt && mv /opt/zabbix-ctl-* /opt/zabbix-ctl
  2. cd /opt/zabbix-ctl
  3. cp config/zabbix-ctl.toml.example zabbix-ctl.toml   # 改数据库、Zabbix API
  4. cp config/env.sh.example config/env.sh && chmod 600 config/env.sh
     编辑填 ZABBIX_CTL_SERVICE_TOKEN、ZBX_TOKEN_DC1
  5. ./start.sh
     或后台: nohup ./start.sh > zabbix-ctl.log 2>&1 &
  6. curl http://127.0.0.1:8090/health

用 systemd（可选）
  cp systemd/zabbix-ctl.service /etc/systemd/system/
  systemctl daemon-reload && systemctl enable --now zabbix-ctl

说明
  - 默认只监听 127.0.0.1:8090，仅给 Gateway 内网调用
  - 数据库连 MeridianOps 同一 MySQL（meridianops 库）
  - Gateway 侧 MERIDIANOPS_ZABBIX_CTL_TOKEN 必须与 ZABBIX_CTL_SERVICE_TOKEN 相同
  - 真实令牌只放 config/env.sh，不要提交仓库
EOF

mkdir -p dist
tar -C dist -czf "dist/${PACKAGE_NAME}.tar.gz" "${PACKAGE_NAME}"
ls -lh "dist/${PACKAGE_NAME}.tar.gz"
echo "staged ${PACKAGE_NAME} (${PACKAGE_LABEL})"
