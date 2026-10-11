#!/usr/bin/env bash
# Installs the scripts into /opt/radar and restarts the services.
# Run from the repo folder:  sudo ./deploy.sh
# The current scripts are kept in /opt/radar/backup-<time>/ for rollback.
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
DEST=/opt/radar
SCRIPTS="common.py ai.py radar.py whales.py heat.py attention.py news.py flows.py brain.py chain.py pulse.py"
SERVICES="radar whales heat attention news flows brain chain pulse"

if [ "$(id -u)" -ne 0 ]; then
    echo "Run with sudo: sudo $0" >&2
    exit 1
fi

# Check the code before touching the running services
for f in $SCRIPTS; do
    # Syntax check only: py_compile would leave root-owned __pycache__ in the repo
    python3 -c 'import ast, sys; ast.parse(open(sys.argv[1]).read(), sys.argv[1])' "$SRC/$f"
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

# The AI summaries need the official Anthropic SDK; everything else runs without it
if ! python3 -c "import anthropic" 2>/dev/null; then
    echo "Note: the anthropic package is not installed, so AI summaries stay off."
    echo "      To turn them on: sudo pip3 install anthropic, and set ANTHROPIC_API_KEY in radar.env"
fi

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
echo "Follow the logs with:  journalctl -u radar -u whales -u heat -u attention -u news -u flows -u brain -u chain -u pulse -f"
