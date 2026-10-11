#!/usr/bin/env bash
# Installs the scripts into /opt/radar and restarts the services.
# Run from the repo folder:  sudo ./deploy.sh
# The current scripts are kept in /opt/radar/backup-<time>/ for rollback.
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
DEST=/opt/radar
SCRIPTS="common.py radar.py whales.py heat.py attention.py"
SERVICES="radar whales heat attention"

if [ "$(id -u)" -ne 0 ]; then
    echo "Run with sudo: sudo $0" >&2
    exit 1
fi

# Check the code before touching the running services
for f in $SCRIPTS; do
    python3 -m py_compile "$SRC/$f"
done

id radar >/dev/null 2>&1 || useradd --system --home-dir "$DEST" --shell /usr/sbin/nologin radar
install -d -o radar -g radar -m 750 "$DEST"

backup="$DEST/backup-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$backup"
cp -p "$DEST"/*.py "$backup"/ 2>/dev/null || true
chown -R radar:radar "$backup"
echo "Old scripts saved in $backup"

for f in $SCRIPTS; do
    install -o radar -g radar -m 640 "$SRC/$f" "$DEST/$f"
done

if [ -f "$DEST/radar.env" ]; then
    chown radar:radar "$DEST/radar.env"
    chmod 600 "$DEST/radar.env"
else
    install -o radar -g radar -m 600 "$SRC/radar.env.example" "$DEST/radar.env"
    echo "Created $DEST/radar.env from the example: fill in your tokens, then run again."
    exit 1
fi

for s in $SERVICES; do
    install -m 644 "$SRC/systemd/$s.service" "/etc/systemd/system/$s.service"
done
systemctl daemon-reload
for s in $SERVICES; do
    systemctl enable --quiet "$s"
    systemctl restart "$s"
done

sleep 3
systemctl --no-pager --lines=0 status $SERVICES || true
echo
echo "Follow the logs with:  journalctl -u radar -u whales -u heat -u attention -f"
