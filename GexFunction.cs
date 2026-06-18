using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Threading.Tasks;
using Alpaca.Markets;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

/// <summary>
/// GexFunction — Net Gamma Exposure (GEX) directional call strategy.
///
/// Concept:
///   Market makers (dealers) who sold options must delta-hedge their exposure.
///   Their hedging direction amplifies or dampens price moves depending on
///   their net gamma position per strike:
///
///     Negative net GEX (dealer short gamma) →
///       Dealers buy as price falls, sell as price rises → moves amplified.
///       Good environment for directional long-call entries.
///
///     Positive net GEX "wall" above current price →
///       Large dealer long-gamma at that strike → price tends to pin there.
///       Acts as a profit-take target; momentum stalls at / past the wall.
///
///   Strategy:
///     Entry  — Every 5 minutes, for each ticker:
///                1. Fetch full option chain (calls + puts, ±10% strikes, 0-30 DTE).
///                2. Compute Net Gamma Proxy per strike:
///                     Σ(Call Gamma) − Σ(Put Gamma)   [proxy for true OI-weighted GEX]
///                3. SKIP if current price is in a positive GEX zone
///                   (pinning / mean-reversion environment — no edge for directional calls).
///                4. SKIP if nearest positive GEX wall above price is &lt; 2% away
///                   (insufficient room for the trade to run before stalling).
///                5. Buy 1 near-ATM call with best gamma/ask score, ≤ 10 DTE.
///
///     Exit   — Checked every 1 minute (GexExitCheck timer) once a position is open:
///                • Stop loss   — close if premium falls 20% from entry.
///                • GEX wall    — close when underlying comes within 0.3% of the
///                                wall level captured at entry (lock in gain before pinning).
///                • EOD         — force-close at 15:50 ET regardless of signal
///                                (avoids overnight gap / assignment risk).
///
/// Runs:
///   GexFunction    — every 5 minutes, 9:45–15:55 ET, weekdays.
///   GexExitCheck   — every 1 minute, same window, skips if no open positions.
/// </summary>
public class GexFunction
{
    private static readonly TimeZoneInfo ET =
        TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

    // ── Tuneable parameters ───────────────────────────────────────────────────
    // Sized for a ~$2 000 account, max $1 000 per trade in REAL premium:
    //   • Min ask $3/share ($300/contract) — no worthless cheap OTM junk
    //   • Max ask $10/share ($1 000/contract) — hard cap at $1 000 spend
    //   • Delta 0.40–0.70 — near-ATM calls with real directional exposure
    //   • Strike -3% to +2% — slightly ITM to just OTM (quality, not lottery)
    //
    //   Math example (TSLA @ $300, budget $1 000):
    //     • $5.00 ask × 100 = $500 contract  ← typical entry
    //     • Stop loss 20% → max loss $100 on that contract
    //     • GEX wall exit locks in gain before pinning; EOD if still open
    private const decimal MaxSpreadPct      = 0.10m;  // 10% spread — tighter; real premium options have tighter markets
    private const decimal MaxRiskPct        = 0.50m;  // 50% of cash = $1 000 on $2 000 account
    private const decimal StopLossPct       = 0.20m;  // 20% stop — real premium, so 20% is a meaningful $ loss
    private const decimal MinWallDistance   = 0.02m;  // skip if GEX wall < 2% above price
    private const decimal WallExitBuffer    = 0.003m; // exit when underlying within 0.3% of GEX wall

    private static readonly TimeSpan EodForceCloseTime = new(15, 50, 0);

    private readonly IAlpacaTradingClient         _tradingClient;
    private readonly IAlpacaDataClient            _dataClient;
    private readonly IAlpacaOptionsDataClient     _optionsDataClient;
    private readonly IConfiguration               _config;
    private readonly ILogger<GexFunction>         _logger;
    private readonly GexPositionStore             _positionStore;
    private readonly RiskState                    _riskState;
    // Cross-strategy coordination — skip entry if another strategy is already
    // in a position on the same ticker (prevents doubling up on the same name).
    private readonly WallBouncePositionStore      _wallBounceStore;

    public GexFunction(
        IAlpacaTradingClient tradingClient,
        IAlpacaDataClient dataClient,
        IAlpacaOptionsDataClient optionsDataClient,
        IConfiguration config,
        ILogger<GexFunction> logger,
        GexPositionStore positionStore,
        RiskState riskState,
        WallBouncePositionStore wallBounceStore)
    {
        _tradingClient   = tradingClient;
        _dataClient      = dataClient;
        _optionsDataClient = optionsDataClient;
        _config          = config;
        _logger          = logger;
        _positionStore   = positionStore;
        _riskState       = riskState;
        _wallBounceStore = wallBounceStore;
    }

    // ── Entry scan — every 5 minutes ─────────────────────────────────────────

    [Function("GexScan")]
    public async Task RunScan([TimerTrigger("30 */5 * * * *", UseMonitor = false)] TimerInfo timer)
    {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, ET);
        _logger.LogInformation("GexScan: tick at {Time} ET ({Day}).", now.ToString("HH:mm:ss"), now.DayOfWeek);

        if (!IsActiveSession(now, "GexScan")) return;

        decimal maxSpreadPct = (decimal)_config.GetValue<double>("GEX_MAX_SPREAD_PCT", (double)MaxSpreadPct);
        decimal maxRiskPct   = (decimal)_config.GetValue<double>("GEX_MAX_RISK_PCT",   (double)MaxRiskPct);

        foreach (var ticker in Tickers.GexWatchlist)
        {
            try
            {
                await ProcessTickerAsync(ticker, now, maxSpreadPct, maxRiskPct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GexScan [{Ticker}]: unhandled error — skipping ticker.", ticker);
            }
        }

        _logger.LogInformation("GexScan: ── scan complete at {Time} ET ──", now.ToString("HH:mm:ss"));
    }

    // ── Exit check — every 1 minute ──────────────────────────────────────────

    [Function("GexExitCheck")]
    public async Task RunExitCheck([TimerTrigger("10 * * * * *", UseMonitor = false)] TimerInfo timer)
    {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, ET);

        if (!IsActiveSession(now, "GexExitCheck")) return;

        if (await _positionStore.IsEmptyAsync()) return;

        foreach (var ticker in await _positionStore.GetOpenTickersAsync())
        {
            _logger.LogInformation("GexExitCheck [{Ticker}]: 1-min exit check.", ticker);
            try
            {
                await CheckExitAsync(ticker, now);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GexExitCheck [{Ticker}]: unhandled error.", ticker);
            }
        }
    }

    // ── Session guard (shared by both timers) ─────────────────────────────────

    private bool IsActiveSession(DateTime now, string tag)
    {
        if (now.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
            return false;

        if (MarketCalendar.IsHoliday(now))
        {
            _logger.LogInformation("{Tag}: {Date} is a market holiday — nothing to do.", tag, now.ToString("yyyy-MM-dd"));
            return false;
        }

        var open  = now.Date.AddHours(9).AddMinutes(45);
        var close = now.Date.AddHours(15).AddMinutes(55);

        if (now < open || now > close)
            return false;

        return true;
    }

    // ── Per-ticker scan (entry + EOD) ─────────────────────────────────────────

    private async Task ProcessTickerAsync(string ticker, DateTime now, decimal maxSpreadPct, decimal maxRiskPct)
    {
        var pos = await _positionStore.TryGetAsync(ticker);

        // ── EOD force-close ──────────────────────────────────────────────────
        if (pos is not null && now.TimeOfDay >= EodForceCloseTime)
        {
            var eodQuote = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
            decimal eodExit = eodQuote?.Bid ?? pos.EntryPremium;
            _logger.LogInformation(
                "GexScan [{Ticker}]: EOD FORCE CLOSE at {Time} ET.", ticker, now.ToString("HH:mm"));
            await ClosePositionAsync(ticker, pos, now, "EOD FORCE CLOSE", eodExit);
            return;
        }

        // Already in a position — exit checks handled by GexExitCheck every minute
        if (pos is not null)
        {
            _logger.LogInformation("GexScan [{Ticker}]: open position exists — skipping entry scan.", ticker);
            return;
        }

        // ── Risk circuit breaker ─────────────────────────────────────────────
        var today = DateOnly.FromDateTime(now);
        if (await _riskState.IsHaltedAsync(today))
        {
            _logger.LogWarning(
                "GexScan [{Ticker}]: SKIP entry — risk circuit breaker halted: {Reason}.",
                ticker, await _riskState.GetHaltReasonAsync(today));
            return;
        }

        // ── Cross-strategy coordination ──────────────────────────────────────
        // Skip entry if WallBounce already holds a position on this ticker.
        if (await _wallBounceStore.HasOpenPositionAsync(ticker))
        {
            _logger.LogInformation(
                "GexScan [{Ticker}]: SKIP entry — WallBounce already has an open position on this ticker.", ticker);
            return;
        }

        // ── Fetch full chain for GEX analysis + call selection ───────────────
        var chain = await FetchFullChainAsync(ticker, now);
        if (chain is null) return;

        // ── Get current underlying price from chain strike distribution ───────
        decimal underlyingPrice = await GetLatestUnderlyingPriceAsync(ticker);
        if (underlyingPrice <= 0m)
        {
            _logger.LogWarning("GexScan [{Ticker}]: could not fetch underlying price — skipping.", ticker);
            return;
        }

        // ── Net GEX analysis ─────────────────────────────────────────────────
        var gex = GexAnalyzer.Analyse(chain.Items, underlyingPrice);

        _logger.LogInformation(
            "GexScan [{Ticker}]: price=${Price:F2}  negativeZone={Neg}  nearestWall=${Wall:F2}  " +
            "wallDist={Dist:P1}  flip=${Flip:F2}.",
            ticker, underlyingPrice, gex.IsNegativeGammaZone, gex.NearestWallAbove,
            gex.WallDistance, gex.FlipLevel);

        // Gate: must be in negative GEX zone (dealers short gamma → moves amplified)
        if (!gex.IsNegativeGammaZone)
        {
            _logger.LogInformation(
                "GexScan [{Ticker}]: SKIP — price in positive GEX zone (pinning likely, no directional edge).", ticker);
            return;
        }

        // Gate: need room to run before the next wall stops momentum
        if (gex.NearestWallAbove > 0m && gex.WallDistance < MinWallDistance)
        {
            _logger.LogInformation(
                "GexScan [{Ticker}]: SKIP — GEX wall ${Wall:F2} only {Dist:P1} above price — insufficient room.",
                ticker, gex.NearestWallAbove, gex.WallDistance);
            return;
        }

        // ── Momentum confirmation ─────────────────────────────────────────────
        // Negative GEX means moves are amplified — but amplified in WHICH direction?
        // We only want calls when the underlying is actually trending UP.
        // Require: last completed 1-min bar is green (close > open).
        // This filters out "falling knife" setups where negative GEX is amplifying
        // a downmove — exactly when a long call would be destroyed.
        var momentum = await GetMomentumAsync(ticker);
        if (momentum is null)
        {
            _logger.LogWarning("GexScan [{Ticker}]: SKIP — could not determine momentum direction.", ticker);
            return;
        }
        if (!momentum.Value.IsBullish)
        {
            _logger.LogInformation(
                "GexScan [{Ticker}]: SKIP — last bar is bearish (open=${Open:F2} close=${Close:F2}) " +
                "— negative GEX may be amplifying downside, not a call setup.",
                ticker, momentum.Value.Open, momentum.Value.Close);
            return;
        }
        _logger.LogInformation(
            "GexScan [{Ticker}]: momentum ✔ bullish — last bar open=${Open:F2} close=${Close:F2} " +
            "(+{Move:P2})  underlying=${Price:F2}.",
            ticker, momentum.Value.Open, momentum.Value.Close,
            (momentum.Value.Close - momentum.Value.Open) / momentum.Value.Open, underlyingPrice);

        // ── Account / risk gates ─────────────────────────────────────────────
        var positions = await _tradingClient.ListPositionsAsync();
        bool alreadyIn = positions.Any(p =>
            p.Symbol.StartsWith(ticker, StringComparison.OrdinalIgnoreCase)
         && p.Symbol.Length > ticker.Length + 2);

        if (alreadyIn)
        {
            _logger.LogInformation("GexScan [{Ticker}]: SKIP — Alpaca already shows an option position.", ticker);
            return;
        }

        var     account = await _tradingClient.GetAccountAsync();
        decimal cash    = account.TradableCash;

        await _riskState.EnsureDailyStartCashAsync(today, cash);

        _logger.LogInformation(
            "GexScan [{Ticker}]: cash=${Cash:F2}  buyingPower=${BP:F2}.", ticker, cash, account.BuyingPower ?? 0m);

        if (cash <= 0m)
        {
            _logger.LogWarning("GexScan [{Ticker}]: SKIP — no tradable cash.", ticker);
            return;
        }

        decimal maxSpreadPctEff = (decimal)_config.GetValue<double>("GEX_MAX_SPREAD_PCT", (double)maxSpreadPct);
        decimal maxRiskPctEff   = (decimal)_config.GetValue<double>("GEX_MAX_RISK_PCT",   (double)maxRiskPct);
        decimal riskBudget      = cash * maxRiskPctEff;
        decimal maxAsk          = Math.Floor(riskBudget / 100m * 100m) / 100m;

        // maxAsk = per-share ask price limit.  e.g. $2 000 × 50% = $1 000 budget → $10/share max.
        // Floor at $1/share ($100/contract).
        if (maxAsk < 1.00m)
        {
            _logger.LogWarning(
                "GexScan [{Ticker}]: SKIP — risk budget ${Budget:F2} too small (maxAsk=${Max:F2}/share).",
                ticker, riskBudget, maxAsk);
            return;
        }

        // ── Select best call from the chain ───────────────────────────────────
        var best = SelectBestCall(ticker, chain.Items, underlyingPrice, maxAsk, maxSpreadPctEff, now);
        if (best is null)
        {
            _logger.LogWarning(
                "GexScan [{Ticker}]: SKIP — no call meets delta/spread/budget criteria.", ticker);
            return;
        }

        if (cash < best.Value.Ask * 100m)
        {
            _logger.LogWarning(
                "GexScan [{Ticker}]: SKIP — insufficient cash ${Cash:F2} < contract cost ${Cost:F2}.",
                ticker, cash, best.Value.Ask * 100m);
            return;
        }

        await PlaceBuyOrderAsync(ticker, best.Value.Symbol, underlyingPrice, best.Value.Ask, now, gex.NearestWallAbove);
    }

    // ── Exit logic (runs every 1 min via GexExitCheck) ───────────────────────

    private async Task CheckExitAsync(string ticker, DateTime now)
    {
        var pos = await _positionStore.TryGetAsync(ticker);
        if (pos is null) return;

        // EOD force-close
        if (now.TimeOfDay >= EodForceCloseTime)
        {
            var eodQuote = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
            decimal eodExit = eodQuote?.Bid ?? pos.EntryPremium;
            _logger.LogInformation(
                "GexExitCheck [{Ticker}]: EOD FORCE CLOSE at {Time} ET.", ticker, now.ToString("HH:mm"));
            await ClosePositionAsync(ticker, pos, now, "EOD FORCE CLOSE", eodExit);
            return;
        }

        var quote = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
        if (quote is null)
        {
            _logger.LogWarning(
                "GexExitCheck [{Ticker}]: could not fetch quote for {Symbol} — skipping.", ticker, pos.OptionSymbol);
            return;
        }

        decimal currentPremium = quote.Value.Bid;
        if (currentPremium <= 0m)
        {
            _logger.LogWarning(
                "GexExitCheck [{Ticker}]: bid is ${Bid:F2} — skipping exit check.", ticker, currentPremium);
            return;
        }

        decimal pnlPct = (currentPremium - pos.EntryPremium) / pos.EntryPremium;

        // ── Stop loss — premium down StopLossPct from entry ─────────────────
        if (pnlPct <= -StopLossPct)
        {
            _logger.LogInformation(
                "GexExitCheck [{Ticker}]: EXIT — STOP LOSS. premium ${Current:F2} ({Pnl:P1}) vs entry ${Entry:F2}.",
                ticker, currentPremium, pnlPct, pos.EntryPremium);
            await ClosePositionAsync(ticker, pos, now, $"STOP LOSS (-{StopLossPct:P0} premium)", currentPremium);
            return;
        }

        // ── GEX wall exit — underlying approaching the pinning zone ──────────
        if (pos.GexWallAbove > 0m)
        {
            decimal underlying = await GetLatestUnderlyingPriceAsync(ticker);
            if (underlying > 0m && underlying >= pos.GexWallAbove * (1m - WallExitBuffer))
            {
                _logger.LogInformation(
                    "GexExitCheck [{Ticker}]: EXIT — GEX WALL REACHED. underlying ${Price:F2} within {Buf:P1} of " +
                    "wall ${Wall:F2}. Taking profit before pinning. premium=${Current:F2} ({Pnl:P1}).",
                    ticker, underlying, WallExitBuffer, pos.GexWallAbove, currentPremium, pnlPct);
                await ClosePositionAsync(ticker, pos, now, $"GEX WALL REACHED (${pos.GexWallAbove:F2})", currentPremium);
                return;
            }
        }

        // Holding — log status
        _logger.LogInformation(
            "GexExitCheck [{Ticker}]: holding {Symbol} — premium=${Current:F2} ({Pnl:P1})  " +
            "entry=${Entry:F2}  gexWall={Wall}.",
            ticker, pos.OptionSymbol, currentPremium, pnlPct, pos.EntryPremium,
            pos.GexWallAbove > 0m ? $"${pos.GexWallAbove:F2}" : "none");
    }

    // ── Chain fetch ───────────────────────────────────────────────────────────

    private async Task<IDictionaryPage<IOptionSnapshot>?> FetchFullChainAsync(string ticker, DateTime now)
    {
        // ±10% strikes + 0-30 DTE to capture all meaningful GEX walls
        var minExpiry = DateOnly.FromDateTime(now.Date);
        var maxExpiry = DateOnly.FromDateTime(now.Date.AddDays(30));

        // We'll use the underlying price from bars to set the range, but since
        // we haven't fetched it yet, use a wide enough range — the GexAnalyzer
        // handles any irrelevant strikes gracefully.
        _logger.LogInformation(
            "GexScan [{Ticker}]: fetching full chain (calls + puts, 0-30 DTE).", ticker);

        try
        {
            var req = new OptionChainRequest(ticker)
            {
                ExpirationDateGreaterThanOrEqualTo = minExpiry,
                ExpirationDateLessThanOrEqualTo    = maxExpiry,
                // No OptionType — need both calls and puts for net GEX
            };

            var page = await _optionsDataClient.GetOptionChainAsync(req);

            _logger.LogInformation(
                "GexScan [{Ticker}]: chain returned {Count} contract(s).", ticker, page.Items.Count);

            if (page.Items.Count == 0)
            {
                _logger.LogWarning("GexScan [{Ticker}]: empty chain.", ticker);
                return null;
            }

            return page;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexScan [{Ticker}]: failed to fetch chain.", ticker);
            return null;
        }
    }

    // ── Call selection ────────────────────────────────────────────────────────

    private (string Symbol, decimal Ask)? SelectBestCall(
        string ticker,
        IReadOnlyDictionary<string, IOptionSnapshot> chain,
        decimal underlyingPrice, decimal maxAsk, decimal maxSpreadPct, DateTime now)
    {
        // $1 000 budget → real premium, near-ATM calls only.
        //   delta 0.40–0.70  — meaningful directional exposure, not a lottery ticket
        //   strike -3% to +2% — slightly ITM to just OTM; avoids worthless far-OTM
        //   0–10 DTE          — short-dated for gamma leverage in a negative-GEX move
        //   Score: gamma/ask  — still maximises movement per dollar paid
        var maxExpiry   = DateOnly.FromDateTime(now.Date.AddDays(10));
        decimal floor   = Math.Round(underlyingPrice * 0.97m, 2);  // up to -3% (slightly ITM)
        decimal ceiling = Math.Round(underlyingPrice * 1.02m, 2);  // up to +2% OTM only

        var candidates = chain
            .Where(kv => GexAnalyzer.IsCall(kv.Key))
            .Select(kv =>
            {
                decimal ask       = kv.Value.Quote?.AskPrice ?? 0m;
                decimal bid       = kv.Value.Quote?.BidPrice ?? 0m;
                decimal delta     = kv.Value.Greeks?.Delta ?? 0m;
                decimal gamma     = kv.Value.Greeks?.Gamma ?? 0m;
                decimal spreadPct = ask > 0m ? (ask - bid) / ask : 1m;
                // Score: gamma/ask — highest ratio = most leverage per dollar spent
                // (the defining edge of cheap OTM calls in negative-GEX environments)
                decimal score     = ask > 0m ? gamma / ask : 0m;
                DateOnly expiry   = ParseExpiry(kv.Key, now);
                decimal strike    = GexAnalyzer.ParseStrike(kv.Key);

                bool meetsCore =
                    ask >= 1.00m && ask <= maxAsk      // $1–$10/share = $100–$1 000/contract
                 && delta >= 0.40m && delta <= 0.70m   // near-ATM — real directional exposure
                 && gamma > 0m
                 && expiry <= maxExpiry                 // 0–10 DTE
                 && strike >= floor && strike <= ceiling; // -3% ITM to +2% OTM

                bool liquid = spreadPct < maxSpreadPct;

                return (Symbol: kv.Key, Ask: ask, Bid: bid, SpreadPct: spreadPct,
                        Delta: delta, Gamma: gamma, Score: score,
                        Strike: strike, Expiry: expiry, MeetsCore: meetsCore, Liquid: liquid);
            })
            .OrderBy(x => x.Expiry)
            .ThenByDescending(x => x.Score)
            .ToList();

        var core = candidates.Where(x => x.MeetsCore).ToList();

        _logger.LogInformation(
            "GexScan [{Ticker}]: call selection — {Pass}/{Total} calls passed core filters " +
            "(ask $1–${Max:F2}/share [$100–${MaxCost:F0}/contract], delta 0.40–0.70, ≤ {Expiry}, strike {Floor:F2}–{Ceil:F2}).",
            ticker, core.Count, candidates.Count, maxAsk, maxAsk * 100m, maxExpiry, floor, ceiling);

        if (core.Count == 0)
        {
            _logger.LogWarning(
                "GexScan [{Ticker}]: no calls met core criteria — " +
                "maxAsk=${Max:F2}/share (${Cost:F2}/contract)  budget based on 5%% of cash.",
                ticker, maxAsk, maxAsk * 100m);
            return null;
        }

        foreach (var expiry in core.Select(x => x.Expiry).Distinct().OrderBy(e => e))
        {
            var liquid = core.Where(x => x.Expiry == expiry && x.Liquid).ToList();

            if (liquid.Count == 0)
            {
                _logger.LogWarning(
                    "GexScan [{Ticker}]: expiry {Expiry} — no liquid calls (spread < {Max:P0}) — trying next.",
                    ticker, expiry, maxSpreadPct);
                continue;
            }

            // Best = highest gamma/ask ratio (most movement per dollar in momentum environment)
            var best = liquid.OrderByDescending(x => x.Score).First();

            _logger.LogInformation(
                "GexScan [{Ticker}]: ★ BEST — {Symbol}  strike=${Strike:F2}  ask=${Ask:F2}/share " +
                "(${ Cost:F2}/contract)  bid=${Bid:F2}  spread={Spread:P1}  " +
                "delta={Delta:F3}  gamma={Gamma:F4}  gamma/ask={Score:F4}  expiry={Expiry}.",
                ticker, best.Symbol, best.Strike, best.Ask, best.Ask * 100m, best.Bid,
                best.SpreadPct, best.Delta, best.Gamma, best.Score, best.Expiry);

            return (best.Symbol, best.Ask);
        }

        _logger.LogWarning("GexScan [{Ticker}]: no liquid call found within 10 DTE.", ticker);
        return null;
    }

    // ── Data helpers ──────────────────────────────────────────────────────────

    private async Task<decimal> GetLatestUnderlyingPriceAsync(string ticker)
    {
        var bars = await FetchRecentBarsAsync(ticker, minutes: 3);
        return bars is not null && bars.Count > 0 ? (decimal)bars[^1].Close : 0m;
    }

    /// <summary>
    /// Momentum gate: checks whether the most recent completed 1-min bar is bullish
    /// (close > open).  Returns null if bars cannot be fetched.
    /// </summary>
    private async Task<(decimal Open, decimal Close, bool IsBullish)?> GetMomentumAsync(string ticker)
    {
        // Fetch last 3 minutes so we always get at least one completed bar
        // (the very latest bar may still be forming, so we use [^1] which is
        //  the last bar returned — Alpaca returns completed bars only).
        var bars = await FetchRecentBarsAsync(ticker, minutes: 3);
        if (bars is null || bars.Count == 0) return null;

        var bar   = bars[^1];
        decimal o = (decimal)bar.Open;
        decimal c = (decimal)bar.Close;
        return (o, c, IsBullish: c > o);
    }

    private async Task<IReadOnlyList<IBar>?> FetchRecentBarsAsync(string ticker, int minutes)
    {
        try
        {
            var utcNow   = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
            var utcStart = DateTime.SpecifyKind(utcNow.AddMinutes(-minutes), DateTimeKind.Utc);
            var req = new HistoricalBarsRequest(
                ticker, utcStart, utcNow,
                new BarTimeFrame(1, BarTimeFrameUnit.Minute))
            { Feed = MarketDataFeed.Sip };

            var page = await _dataClient.ListHistoricalBarsAsync(req);
            return page.Items;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexScan [{Ticker}]: failed to fetch bars.", ticker);
            return null;
        }
    }

    private async Task<(decimal Bid, decimal Ask)?> GetOptionQuoteAsync(string optionSymbol, string ticker)
    {
        try
        {
            var req    = new LatestOptionsDataRequest(new[] { optionSymbol });
            var quotes = await _optionsDataClient.ListLatestQuotesAsync(req);
            if (quotes.TryGetValue(optionSymbol, out var q))
                return (q.BidPrice, q.AskPrice);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexFunction [{Ticker}]: failed to fetch quote for {Symbol}.", ticker, optionSymbol);
        }
        return null;
    }

    // ── Order helpers ─────────────────────────────────────────────────────────

    private async Task PlaceBuyOrderAsync(
        string ticker, string optionSymbol, decimal underlyingPrice,
        decimal premium, DateTime now, decimal gexWallAbove)
    {
        try
        {
            _logger.LogInformation(
                "GexScan [{Ticker}]: submitting BUY — symbol={Symbol}  qty=1  Market  Day  gexWall={Wall}.",
                ticker, optionSymbol, gexWallAbove > 0m ? $"${gexWallAbove:F2}" : "none");

            var req   = new NewOrderRequest(optionSymbol, 1, OrderSide.Buy, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "GexScan [{Ticker}]: ✔ BUY accepted — orderId={Id}  status={Status}  " +
                "underlying=${Price:F2}  premium(ask)=${Premium:F2}  gexWall={Wall}  " +
                "exit: -{Stop:P0} stop loss · GEX wall · EOD close.",
                ticker, order.OrderId, order.OrderStatus, underlyingPrice, premium,
                gexWallAbove > 0m ? $"${gexWallAbove:F2}" : "none", StopLossPct);

            await _positionStore.OpenAsync(ticker, optionSymbol, premium, underlyingPrice, now, gexWallAbove);

            decimal? strike = ParseStrike(optionSymbol);
            await SendAlertAsync(ticker, optionSymbol, underlyingPrice, "ENTRY", now, isEntry: true,
                strike: strike, premium: premium, gexWallAbove: gexWallAbove);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexScan [{Ticker}]: ✘ FAILED to place buy order for {Symbol}.", ticker, optionSymbol);
        }
    }

    private async Task ClosePositionAsync(
        string ticker, GexPositionStore.Position pos, DateTime now, string reason, decimal exitPremium)
    {
        try
        {
            _logger.LogInformation(
                "GexFunction [{Ticker}]: placing SELL — symbol={Symbol}  reason={Reason}.",
                ticker, pos.OptionSymbol, reason);

            var req   = new NewOrderRequest(pos.OptionSymbol, 1, OrderSide.Sell, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "GexFunction [{Ticker}]: ✔ SELL accepted — orderId={Id}  status={Status}  " +
                "entryPremium=${Entry:F2}  exitPremium=${Exit:F2}.",
                ticker, order.OrderId, order.OrderStatus, pos.EntryPremium, exitPremium);

            await SendAlertAsync(ticker, pos.OptionSymbol, pos.EntryUnderlying, reason, now, isEntry: false,
                entryPremium: pos.EntryPremium, exitPremium: exitPremium);

            decimal pnlDollars  = (exitPremium - pos.EntryPremium) * 100m;
            bool    openedToday = pos.EntryTime.Date == now.Date;
            await _riskState.RecordTradeClosedAsync(DateOnly.FromDateTime(now), pnlDollars, openedToday, "GexFunction");

            await _positionStore.CloseAsync(ticker);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "GexFunction [{Ticker}]: ✘ FAILED to close {Symbol} — position may still be open!",
                ticker, pos.OptionSymbol);
        }
    }

    // ── OCC symbol helpers ────────────────────────────────────────────────────

    private static DateOnly ParseExpiry(string occSymbol, DateTime now)
    {
        try
        {
            int i = 0;
            while (i < occSymbol.Length && !char.IsDigit(occSymbol[i])) i++;
            if (i + 6 <= occSymbol.Length)
            {
                int yy = int.Parse(occSymbol.Substring(i, 2));
                int mm = int.Parse(occSymbol.Substring(i + 2, 2));
                int dd = int.Parse(occSymbol.Substring(i + 4, 2));
                return new DateOnly(2000 + yy, mm, dd);
            }
        }
        catch { }
        return DateOnly.FromDateTime(now.Date);
    }

    private static decimal? ParseStrike(string occSymbol)
    {
        try
        {
            int i = 0;
            while (i < occSymbol.Length && !char.IsDigit(occSymbol[i])) i++;
            int start = i + 7;
            if (start + 8 <= occSymbol.Length
             && long.TryParse(occSymbol.Substring(start, 8), out long raw))
                return raw / 1000m;
        }
        catch { }
        return null;
    }

    // ── Email alerts ──────────────────────────────────────────────────────────

    private async Task SendAlertAsync(
        string ticker, string optionSymbol, decimal underlyingPrice,
        string reason, DateTime now, bool isEntry,
        decimal? strike = null, decimal? premium = null, decimal gexWallAbove = 0m,
        decimal? entryPremium = null, decimal? exitPremium = null)
    {
        string sender     = Environment.GetEnvironmentVariable("GMAIL_SENDER")     ?? "";
        string password   = Environment.GetEnvironmentVariable("GMAIL_PASSWORD")   ?? "";
        string recipients = Environment.GetEnvironmentVariable("ALERT_RECIPIENTS") ?? "";

        if (string.IsNullOrEmpty(sender) || string.IsNullOrEmpty(recipients))
        {
            _logger.LogWarning("GexFunction [{Ticker}]: email not configured — skipping alert.", ticker);
            return;
        }

        string action = isEntry ? "Call Entry (GEX)" : $"Call Closed — {reason}";
        string color  = isEntry ? "#0d6efd"
                      : reason.StartsWith("STOP") || reason.StartsWith("EOD") ? "#c0392b" : "#1a7a1a";

        decimal? pnlPct = (!isEntry && entryPremium.HasValue && exitPremium.HasValue && entryPremium.Value != 0m)
            ? (exitPremium.Value - entryPremium.Value) / entryPremium.Value
            : null;

        string body = $"""
            <html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">
            <h2 style="color:{color}">GexFunction — {ticker} {action}</h2>
            <table style="border-collapse:collapse;background:#fff;border-radius:8px;padding:16px;min-width:340px">
              <tr><td style="padding:8px 16px;color:#555">Ticker</td>
                  <td style="padding:8px 16px;font-weight:bold">{ticker}</td></tr>
              <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Time (ET)</td>
                  <td style="padding:8px 16px;font-weight:bold">{now:HH:mm:ss}</td></tr>
              <tr><td style="padding:8px 16px;color:#555">Option Symbol</td>
                  <td style="padding:8px 16px;font-weight:bold;font-family:monospace">{optionSymbol}</td></tr>
              <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">{ticker} Price</td>
                  <td style="padding:8px 16px;font-weight:bold">${underlyingPrice:F2}</td></tr>
              {(isEntry
                ? $"""
                  <tr><td style="padding:8px 16px;color:#555">Strike</td>
                      <td style="padding:8px 16px;font-weight:bold">{(strike.HasValue ? $"${strike.Value:F2}" : "n/a")}</td></tr>
                  <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Premium (Ask)</td>
                      <td style="padding:8px 16px;font-weight:bold">{(premium.HasValue ? $"${premium.Value:F2}" : "n/a")}  (cost: {(premium.HasValue ? $"${premium.Value * 100m:F2}" : "n/a")})</td></tr>
                  <tr><td style="padding:8px 16px;color:#555">GEX Wall Target</td>
                      <td style="padding:8px 16px;font-weight:bold;color:#0d6efd">{(gexWallAbove > 0m ? $"${gexWallAbove:F2}" : "none identified")}</td></tr>
                  <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Exit Plan</td>
                      <td style="padding:8px 16px;color:#c0392b;font-weight:bold">
                        Stop loss -{StopLossPct:P0} premium · GEX wall exit · EOD force-close at {EodForceCloseTime}
                      </td></tr>
                  """
                : $"""
                  <tr><td style="padding:8px 16px;color:#555">Close Reason</td>
                      <td style="padding:8px 16px;font-weight:bold">{reason}</td></tr>
                  <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Entry → Exit Premium</td>
                      <td style="padding:8px 16px;font-weight:bold">
                        {(entryPremium.HasValue ? $"${entryPremium.Value:F2}" : "n/a")} → {(exitPremium.HasValue ? $"${exitPremium.Value:F2}" : "n/a")}
                        {(pnlPct.HasValue ? $" ({pnlPct.Value:P1})" : "")}
                      </td></tr>
                  """)}
            </table>
            <p style="color:#888;font-size:12px;margin-top:16px">
              GexFunction · Qty: 1 contract · Entry: Market · Exit: Market · Day ·
              Scan: every 5 min, 9:45–15:55 ET · Exit check: every 1 min ·
              Risk cap: {MaxRiskPct:P0} of cash · Max spread: {MaxSpreadPct:P0} ·
              Stop loss: -{StopLossPct:P0} · Min GEX wall distance: {MinWallDistance:P0}
            </p>
            </body></html>
            """;

        using var msg = new MailMessage();
        msg.From       = new MailAddress(sender);
        msg.Subject    = $"GexFunction {ticker} {action} — {optionSymbol} @ {now:HH:mm} ET";
        msg.Body       = body;
        msg.IsBodyHtml = true;
        foreach (var r in recipients.Split(',', StringSplitOptions.RemoveEmptyEntries))
            msg.To.Add(r.Trim());

        using var smtp = new SmtpClient("smtp.gmail.com", 587)
        {
            Credentials = new NetworkCredential(sender, password),
            EnableSsl   = true,
        };

        try
        {
            await smtp.SendMailAsync(msg);
            _logger.LogInformation(
                "GexFunction [{Ticker}]: email sent — {Action}.", ticker, action);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexFunction [{Ticker}]: failed to send alert email.", ticker);
        }
    }
}
