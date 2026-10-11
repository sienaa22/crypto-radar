# Crypto Radar

Four small services that watch public data for early crypto signals and send
them to Telegram. Python 3.8+ standard library only, no packages to install.

| Service | Script | What it sends |
|---|---|---|
| Project Radar | `radar.py` | New projects in DefiLlama adapter PRs (TVL and fees/volume), new audit contests (Code4rena, Sherlock, CodeHawks, Hats Finance, Cantina), new Immunefi programs, and one daily status report for all four services |
| Whale Watch | `whales.py` | Big trades, quiet accumulation and liquidations of Hyperliquid whales and smart traders, rated by signal quality, plus smart-money consensus |
| Heat Analyzer | `heat.py` | Daily list of DeFi protocols whose fees grow faster than their token price moved |
| Attention Radar | `attention.py` | Daily list of coins that several independent sources point at |

`common.py` holds the shared code: config, HTTP with retries, Telegram, and
the SQLite outbox. Every message goes through the outbox and is marked sent
only after Telegram accepts it, so an outage delays alerts instead of losing them.

## Whale Watch signal quality

A large position is often not a bet on price. Market makers carry inventory,
and many big shorts are hedges against spot held elsewhere or funding-rate
farms. Before a BIG TRADE is sent, the trader is checked:

- **PnL**: all-time, 30 days and 7 days, the open PnL and funding of the position,
  and the trader's closed-trade record on that coin
- **Not directional**: market maker (30-day volume far above account size, or
  mostly maker orders), short covered by spot on Hyperliquid, market-neutral
  book (longs about equal to shorts), funding farm (low leverage on the side
  that earns funding)
- **Conviction**: position size compared to the account

The result is HIGH, MEDIUM or LOW. Trades below `HL_MIN_QUALITY` (default
MEDIUM) are logged but not sent. Every big trade is priced again 24h and 72h
later; the alert shows the trader's earlier hit rate, and
`python3 whales.py --report` compares the quality levels. If LOW trades do as
well as HIGH ones, the filter is not helping and should be adjusted.

Hedges on other exchanges cannot be seen from Hyperliquid data. The
per-trader track record is the check for those.

## Install or update on the server

```bash
git clone <this repo> ~/radar && cd ~/radar
sudo ./deploy.sh
```

`deploy.sh` checks the code compiles, keeps the old scripts in
`/opt/radar/backup-<time>/`, installs the scripts and the systemd units, and
restarts the services. The first run creates `/opt/radar/radar.env` from
`radar.env.example` if it is missing; every setting is listed there.

Useful commands:

```bash
journalctl -u radar -u whales -u heat -u attention -f
sudo -u radar python3 /opt/radar/radar.py --env /opt/radar/radar.env --status
sudo -u radar python3 /opt/radar/whales.py --env /opt/radar/radar.env --trader 0x...
sudo -u radar python3 /opt/radar/whales.py --env /opt/radar/radar.env --report
sudo -u radar python3 /opt/radar/heat.py --env /opt/radar/radar.env --report
```

Existing databases are upgraded in place on the first start.

## Tests

```bash
python3 -m unittest discover -s tests
```

The tests use fake APIs and a local HTTP server; they need no network access.
