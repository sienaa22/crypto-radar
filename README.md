# Crypto Radar

Seven small services that watch public data for early crypto signals and send
them to Telegram. Python 3.8+ standard library only, no packages to install.

| Service | Script | What it sends |
|---|---|---|
| Project Radar | `radar.py` | New projects in DefiLlama adapter PRs (TVL and fees/volume), new audit contests (Code4rena, Sherlock, CodeHawks, Hats Finance, Cantina), new Immunefi programs, and one daily status report for all services |
| Whale Watch | `whales.py` | Big trades, quiet accumulation and liquidations of Hyperliquid whales and smart traders, rated by signal quality, plus smart-money consensus |
| Heat Analyzer | `heat.py` | Daily list of DeFi protocols whose fees grow faster than their token price moved |
| Attention Radar | `attention.py` | Daily list of coins that several independent sources point at |
| Flow Watch | `flows.py` | Binance futures positioning (open-interest build-ups, crowded funding, top traders against the crowd) and USDT/USDC supply read from Ethereum and Tron |
| Signal Hub | `brain.py` | Prices every signal from every service, scores each kind against BTC after 1 and 7 days, learns weights from that, and sends CONVERGENCE alerts when independent signals line up on one coin |
| News Watch | `news.py` | Token unlocks (large cliffs ahead, investor/team vesting ending), exchange listings (Binance, Upbit, Bybit, OKX, Coinbase), hacks, and important headlines from four news sites |

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

## Signal Hub: measuring and combining

Every service writes what it alerted on to one shared ledger (`signals.db`):
the kind of signal, the coin, and whether it points up, down, or only means
attention. The hub records the price when it fired and again after 1 and 7
days, next to BTC. The weekly scorecard (Mondays, or `python3 brain.py
--scorecard`) shows per kind how often it was right and by how much it beat
BTC, and labels it USEFUL, noise, or INVERSE (reliably wrong, so worth fading).

From the scorecard each kind gets a weight: 1.0 until it has a record, up to
2.0 when it beats BTC, down to 0.2 when it does not. When at least 3 signals
from at least 2 services land on the same coin within 48 hours, the hub sends
one CONVERGENCE alert with the weighted direction. Convergences are measured
too, so the scorecard shows whether combining signals actually helps.

`python3 brain.py --coin APT` lists everything recorded on one coin.

## AI summaries (optional)

With `pip3 install anthropic` and `ANTHROPIC_API_KEY` in `radar.env`, sent
news and convergence alerts get a short Persian summary with the likely
impact, and the AI's direction is used for the headline's signal. Without
them, alerts go out unchanged. `AI_MAX_CALLS_PER_DAY` caps the cost.

## News Watch scoring

Headlines from CoinDesk, The Block, Decrypt and Cointelegraph get points for
what they are about (hack 4, ETF decision 4, insolvency 4, exchange listing 3,
lawsuit 3, unlock 2, tokenomics 2, ...), for naming a top coin, and for being
carried by more than one outlet. Price analysis, opinion and court follow-ups
of old cases lose points. Headlines at `NEWS_MIN_SCORE` (default 4) or more are
sent, once per story. Check a headline with:

```bash
python3 news.py --score "Aptos token unlocks for investors end today"
```

`python3 news.py --unlocks` lists the unlock alerts the cached schedules give.

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
journalctl -u radar -u whales -u heat -u attention -u news -u flows -u brain -f
sudo -u radar python3 /opt/radar/radar.py --env /opt/radar/radar.env --status
sudo -u radar python3 /opt/radar/whales.py --env /opt/radar/radar.env --trader 0x...
sudo -u radar python3 /opt/radar/whales.py --env /opt/radar/radar.env --report
sudo -u radar python3 /opt/radar/heat.py --env /opt/radar/radar.env --report
sudo -u radar python3 /opt/radar/brain.py --env /opt/radar/radar.env --scorecard
```

Existing databases are upgraded in place on the first start.

## Tests

```bash
python3 -m unittest discover -s tests
```

The tests use fake APIs and a local HTTP server; they need no network access.
