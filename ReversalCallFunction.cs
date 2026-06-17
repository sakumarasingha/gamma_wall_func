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
/// ReversalCall — bullish-reversal long-call strategy across TSLA + 10 other
/// super-mega-cap, high-liquidity names.
///
/// Concept:
///   The function runs every 5 minutes (offset 30s past the minute, so it
///   fires at :00:30, :05:30, :10:30, ... e.g. 15:45:30) so open positions
///   get checked for profit/stop frequently.
///
///   Entry — only evaluated on 15-min bar-close ticks (minute % 15 == 0, i.e.
///   :00:30, :15:30, :30:30, :45:30). Looks at just the latest completed
///   15-min bar for a classic hammer / dragonfly-doji bullish reversal
///   (see PatternDetector):
///     • Downtrend leading into the candle
///     • Hammer/dragonfly-doji shape (long lower wick, little/no upper wick)
///     • RSI(14) oversold at the candle
///     • Candle low at/near recent support
///     • Candle volume above the recent average
///
///   When ALL checks pass, buy 1 call contract at market.
///     • Liquidity — bid/ask spread must be &lt; REV_MAX_SPREAD_PCT (default 15%).
///     • Sizing    — contract cost capped at REV_MAX_RISK_PCT (default 50%) of
///                   tradable cash — sized for small accounts (e.g. ~$500).
///     • Strike    — slightly OTM call, nearest liquid expiry (≤10 days out).
///
///   Exit — checked every 5-minute tick. NO VOLD check. Pure premium-based
///   management:
///     • Stop loss     — close if premium falls 15% from entry.
///     • Trailing stop — once premium gains 15% from entry, a trailing stop
///                        activates 5% below the running peak premium; if
///                        premium pulls back to/through that level, close.
///
/// Runs every 5 minutes (offset :30s), 9:45–15:55 ET, Monday–Friday.
/// </summary>
public class ReversalCallFunction
{
    private static readonly TimeZoneInfo ET =
        TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

    // ── Tuneable parameters ───────────────────────────────────────────────────
    // Sized for a ~$2 000 account willing to risk up to $1 000 per trade:
    //   50% of cash = $1 000 max contract cost → max ask up to $10/share
    //   Min ask $3/share ($300/contract) — real premium, no cheap OTM junk
    //   Delta 0.40–0.70 — near-ATM, meaningful directional exposure
    //   Strike -3% to +2% — slightly ITM to just OTM
    private const decimal MaxSpreadPct        = 0.10m;  // 10% spread — tighter; real premium options have tighter markets
    private const decimal MaxRiskPct          = 0.50m;  // 50% of cash per trade ($1 000 on $2 000)
    private const decimal StopLossPct         = 0.20m;  // close if premium falls 20% from entry
    private const decimal TrailingActivatePct = 0.15m;  // trailing stop arms once premium is +15% from entry
    private const decimal TrailingStepPct     = 0.05m;  // once armed, close if premium pulls back 5% from its peak
    private const int     BarsLookback        = 10;     // calendar days of 15-min bars to fetch

    // EOD force-close — if a position is still open at/after this time, close
    // it at market regardless of strategy exit signal (avoids overnight gap /
    // assignment risk). 5 minutes before the 15:55 session close.
    private static readonly TimeSpan EodForceCloseTimeOfDay = new(15, 50, 0);

    private readonly IAlpacaTradingClient            _tradingClient;
    private readonly IAlpacaDataClient               _dataClient;
    private readonly IAlpacaOptionsDataClient        _optionsDataClient;
    private readonly IConfiguration                  _config;
    private readonly ILogger<ReversalCallFunction>   _logger;
    private readonly ReversalPositionStore           _positionStore;
    private readonly RiskState                       _riskState;

    public ReversalCallFunction(
        IAlpacaTradingClient tradingClient,
        IAlpacaDataClient dataClient,
        IAlpacaOptionsDataClient optionsDataClient,
        IConfiguration config,
        ILogger<ReversalCallFunction> logger,
        ReversalPositionStore positionStore,
        RiskState riskState)
    {
        _tradingClient     = tradingClient;
        _dataClient        = dataClient;
        _optionsDataClient = optionsDataClient;
        _config            = config;
        _logger            = logger;
        _positionStore     = positionStore;
        _riskState         = riskState;
    }

    // ── Entry point — every 5 min, offset 30s past the minute ────────────────

    [Function("ReversalCall")]
    public async Task Run([TimerTrigger("30 */5 * * * *")] TimerInfo timer)
    {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, ET);
        _logger.LogInformation(
            "ReversalCall: tick at {Time} ET ({DayOfWeek}).", now.ToString("HH:mm:ss"), now.DayOfWeek);

        // ── Weekend guard ─────────────────────────────────────────────────────
        if (now.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
        {
            _logger.LogInformation("ReversalCall: weekend ({Day}) — markets closed, nothing to do.", now.DayOfWeek);
            return;
        }

        // ── Market holiday guard ─────────────────────────────────────────────
        if (MarketCalendar.IsHoliday(now))
        {
            _logger.LogInformation("ReversalCall: {Date} is a market holiday — nothing to do.", now.ToString("yyyy-MM-dd"));
            return;
        }

        // ── Time window — 9:45 to 15:55 ET ───────────────────────────────────
        var sessionOpen  = now.Date.AddHours(9).AddMinutes(45);
        var sessionClose = now.Date.AddHours(15).AddMinutes(55);

        if (now < sessionOpen || now > sessionClose)
        {
            if (now < sessionOpen)
                _logger.LogInformation(
                    "ReversalCall: {Time} ET — too early, session opens at 09:45. Waiting {Min} min.",
                    now.ToString("HH:mm"), (int)(sessionOpen - now).TotalMinutes);
            else
                _logger.LogInformation(
                    "ReversalCall: {Time} ET — past session close (15:55). No action until tomorrow.",
                    now.ToString("HH:mm"));
            return;
        }

        decimal maxSpreadPct = (decimal)_config.GetValue<double>("REV_MAX_SPREAD_PCT", (double)MaxSpreadPct);
        decimal maxRiskPct   = (decimal)_config.GetValue<double>("REV_MAX_RISK_PCT", (double)MaxRiskPct);

        // 15-min bars close at :00, :15, :30, :45. Since this tick runs 30s after
        // each 5-min mark, a 15-min bar has *just* closed when now.Minute % 15 == 0.
        bool isFifteenMinBarClose = now.Minute % 15 == 0;

        var startUtc = DateTime.SpecifyKind(now.AddDays(-BarsLookback), DateTimeKind.Utc);
        var nowUtc   = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);

        foreach (var ticker in Tickers.ReversalWatchlist)
        {
            await ProcessTickerAsync(ticker, now, startUtc, nowUtc, maxSpreadPct, maxRiskPct, isFifteenMinBarClose);
        }

        _logger.LogInformation("ReversalCall: ── tick complete at {Time} ET ──", now.ToString("HH:mm:ss"));
    }

    // ── Fast exit-only check — every 1 minute, open positions only ──────────
    //
    // The main 5-min tick (above) handles entry scans + exit checks as a
    // backstop. This timer runs every minute and ONLY evaluates exit logic
    // (stop loss / trailing stop) for tickers that currently have an open
    // position — no bar fetch, no pattern scan, no order sizing. This tightens
    // the trailing-stop / stop-loss reaction time from 5 min to ~1 min without
    // changing order types (Alpaca options only support Market/Limit, so the
    // exit itself is still a market sell — just triggered sooner).
    [Function("ReversalExitCheck")]
    public async Task RunExitCheck([TimerTrigger("0 * * * * *")] TimerInfo timer)
    {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, ET);

        if (now.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
            return;

        if (MarketCalendar.IsHoliday(now))
            return;

        var sessionOpen  = now.Date.AddHours(9).AddMinutes(45);
        var sessionClose = now.Date.AddHours(15).AddMinutes(55);
        if (now < sessionOpen || now > sessionClose)
            return;

        if (await _positionStore.IsEmptyAsync())
            return;

        foreach (var ticker in await _positionStore.GetOpenTickersAsync())
        {
            _logger.LogInformation("ReversalExitCheck [{Ticker}]: 1-min exit check.", ticker);
            await CheckExitAsync(ticker, now);
        }
    }

    private async Task ProcessTickerAsync(
        string ticker, DateTime now, DateTime startUtc, DateTime nowUtc, decimal maxSpreadPct, decimal maxRiskPct,
        bool isFifteenMinBarClose)
    {
        bool hasOpenPosition = await _positionStore.HasOpenPositionAsync(ticker);

        // ── EXIT — premium-based stop loss / trailing stop, no VOLD check ────
        // Checked every 5-minute tick so open positions are monitored closely.
        if (hasOpenPosition)
        {
            await CheckExitAsync(ticker, now);
            return;
        }

        // ── ENTRY — only evaluated right after a 15-min bar closes ──────────
        if (!isFifteenMinBarClose)
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: no position — not a 15-min bar-close tick, skipping entry check.", ticker);
            return;
        }

        // ── Look for a bullish reversal pattern on the latest 15-min bar ────
        var bars = await FetchBarsAsync(ticker, startUtc, nowUtc);

        var result = PatternDetector.DetectBullishReversal(bars);

        _logger.LogInformation(
            "ReversalCall [{Ticker}]: {Bars} bar(s) — {Description}.",
            ticker, bars.Count, result.Description);

        if (!result.Detected)
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: SKIP — no bullish reversal pattern on latest bar.", ticker);
            return;
        }

        // ── Risk circuit breaker — skip new entries if halted for the day ────
        var today = DateOnly.FromDateTime(now);
        if (await _riskState.IsHaltedAsync(today))
        {
            string haltReason = await _riskState.GetHaltReasonAsync(today);
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: SKIP entry — risk circuit breaker halted for today: {Reason}.",
                ticker, haltReason);
            return;
        }

        decimal price = result.CandleClose;
        _logger.LogInformation(
            "ReversalCall [{Ticker}]: ★ ENTRY SIGNAL — bullish reversal confirmed ({Description})  price=${Price:F2}.",
            ticker, result.Description, price);

        await TryEnterCallAsync(ticker, price, now, maxSpreadPct, maxRiskPct);
    }

    // ── Exit — stop loss / trailing stop on premium ──────────────────────────

    private async Task CheckExitAsync(string ticker, DateTime now)
    {
        var pos = await _positionStore.TryGetAsync(ticker);
        if (pos is null)
            return;

        // ── EOD force-close — close at market regardless of premium levels,
        // to avoid overnight gap / assignment risk. ──────────────────────────
        if (now.TimeOfDay >= EodForceCloseTimeOfDay)
        {
            var eodQuote = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
            decimal eodExitPremium = eodQuote?.Bid ?? pos.EntryPremium;
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: EOD FORCE CLOSE — {Time} ET ≥ {Eod} cutoff. Closing call.",
                ticker, now.ToString("HH:mm"), EodForceCloseTimeOfDay);
            await ClosePositionAsync(ticker, pos, now, "EOD FORCE CLOSE", eodExitPremium);
            return;
        }

        var quote = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
        if (quote is null)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: holding {Symbol} — could not fetch latest quote, skipping exit check.",
                ticker, pos.OptionSymbol);
            return;
        }

        // Use bid (what we'd actually receive selling) as "current premium".
        decimal currentPremium = quote.Value.Bid;
        if (currentPremium <= 0m)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: holding {Symbol} — bid is ${Bid:F2}, skipping exit check.",
                ticker, pos.OptionSymbol, currentPremium);
            return;
        }

        if (currentPremium > pos.PeakPremium)
            pos.PeakPremium = currentPremium;

        decimal pnlPct = (currentPremium - pos.EntryPremium) / pos.EntryPremium;

        // ── Stop loss — premium down 15% from entry ──────────────────────────
        if (pnlPct <= -StopLossPct)
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: EXIT — STOP LOSS. premium ${Current:F2} is {Pnl:P1} vs entry ${Entry:F2} " +
                "(≤ -{Stop:P0}). Closing call.",
                ticker, currentPremium, pnlPct, pos.EntryPremium, StopLossPct);
            await ClosePositionAsync(ticker, pos, now, "STOP LOSS (-15% premium)", currentPremium);
            return;
        }

        // ── Arm trailing stop once premium is +15% from entry ───────────────
        if (!pos.TrailingActive && currentPremium >= pos.EntryPremium * (1m + TrailingActivatePct))
        {
            pos.TrailingActive = true;
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: trailing stop ARMED — premium ${Current:F2} is {Pnl:P1} vs entry ${Entry:F2} " +
                "(≥ +{Activate:P0}). Will trail {Step:P0} below peak.",
                ticker, currentPremium, pnlPct, pos.EntryPremium, TrailingActivatePct, TrailingStepPct);
        }

        // Persist the (possibly updated) peak premium / trailing-armed flag so a
        // cold-started instance picks up where this one left off.
        await _positionStore.SaveAsync(ticker, pos);

        // ── Trailing stop — pullback of 5% from the peak premium ─────────────
        if (pos.TrailingActive)
        {
            decimal trailLevel = pos.PeakPremium * (1m - TrailingStepPct);
            if (currentPremium <= trailLevel)
            {
                _logger.LogInformation(
                    "ReversalCall [{Ticker}]: EXIT — TRAILING STOP. premium ${Current:F2} ≤ trail level ${Trail:F2} " +
                    "({Step:P0} below peak ${Peak:F2}). Closing call.",
                    ticker, currentPremium, trailLevel, TrailingStepPct, pos.PeakPremium);
                await ClosePositionAsync(ticker, pos, now, "TRAILING STOP (-5% from peak)", currentPremium);
                return;
            }

            _logger.LogInformation(
                "ReversalCall [{Ticker}]: holding {Symbol} — premium ${Current:F2} ({Pnl:P1})  peak=${Peak:F2}  " +
                "trailing stop at ${Trail:F2}.",
                ticker, pos.OptionSymbol, currentPremium, pnlPct, pos.PeakPremium, trailLevel);
        }
        else
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: holding {Symbol} — premium ${Current:F2} ({Pnl:P1})  peak=${Peak:F2}  " +
                "trailing stop not yet armed (needs +{Activate:P0}).",
                ticker, pos.OptionSymbol, currentPremium, pnlPct, pos.PeakPremium, TrailingActivatePct);
        }
    }

    // ── Bar fetch ─────────────────────────────────────────────────────────────

    private async Task<IReadOnlyList<IBar>> FetchBarsAsync(string ticker, DateTime startUtc, DateTime endUtc)
    {
        var request = new HistoricalBarsRequest(
            ticker,
            DateTime.SpecifyKind(startUtc, DateTimeKind.Utc),
            DateTime.SpecifyKind(endUtc,   DateTimeKind.Utc),
            new BarTimeFrame(15, BarTimeFrameUnit.Minute))
        {
            Feed = MarketDataFeed.Sip,
        };

        try
        {
            var page = await _dataClient.ListHistoricalBarsAsync(request);
            _logger.LogDebug("ReversalCall [{Ticker}]: fetched {Count} bar(s).", ticker, page.Items.Count);
            return page.Items;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalCall [{Ticker}]: failed to fetch bars — ticker will be skipped.", ticker);
            return Array.Empty<IBar>();
        }
    }

    // ── Entry ─────────────────────────────────────────────────────────────────

    private async Task TryEnterCallAsync(
        string ticker, decimal currentPrice, DateTime now, decimal maxSpreadPct, decimal maxRiskPct)
    {
        // Gate A — confirm no existing option position via API
        var positions = await _tradingClient.ListPositionsAsync();
        bool alreadyIn = positions.Any(p =>
            p.Symbol.StartsWith(ticker, StringComparison.OrdinalIgnoreCase)
         && p.Symbol.Length > ticker.Length + 2);

        if (alreadyIn)
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: SKIP — Alpaca already shows an option position on this ticker.", ticker);
            return;
        }

        // Gate B — account balance / risk cap
        var     account = await _tradingClient.GetAccountAsync();
        decimal cash    = account.TradableCash;

        _logger.LogInformation(
            "ReversalCall [{Ticker}]: account — tradable cash={Cash:C}  buying power={BP:C}.",
            ticker, cash, account.BuyingPower ?? 0m);

        await _riskState.EnsureDailyStartCashAsync(DateOnly.FromDateTime(now), cash);

        if (cash <= 0m)
        {
            _logger.LogWarning("ReversalCall [{Ticker}]: SKIP — tradable cash is ${Cash:F2}.", ticker, cash);
            return;
        }

        decimal riskBudget = cash * maxRiskPct;
        decimal maxAsk     = Math.Floor(riskBudget / 100m * 100m) / 100m;  // round down to nearest cent

        _logger.LogInformation(
            "ReversalCall [{Ticker}]: risk cap — {RiskPct:P0} of cash ${Cash:F2} = ${Budget:F2} → maxAsk=${MaxAsk:F2} (contract cost ≤ ${Cost:F2}).",
            ticker, maxRiskPct, cash, riskBudget, maxAsk, maxAsk * 100m);

        // Min $1/share ask → $100/contract. If budget can't cover that, skip.
        if (maxAsk < 1.00m)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: SKIP — {RiskPct:P0} risk budget ${Budget:F2} too low " +
                "(maxAsk=${MaxAsk:F2}/share, need ≥ $1.00).",
                ticker, maxRiskPct, riskBudget, maxAsk);
            return;
        }

        // Gate C — find best call option
        var best = await FindBestCallOptionAsync(ticker, currentPrice, maxAsk, maxSpreadPct, now);
        if (best is null)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: SKIP — no call found meeting delta/liquidity/budget/expiry criteria within ${MaxAsk:F2}.",
                ticker, maxAsk);
            return;
        }

        // Gate D — hard cash check
        decimal contractCost = best.Value.Ask * 100m;
        if (cash < contractCost)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: SKIP — insufficient cash ${Cash:F2} < contract cost ${Cost:F2}.",
                ticker, cash, contractCost);
            return;
        }

        await PlaceCallBuyOrderAsync(ticker, best.Value.Symbol, currentPrice, best.Value.Ask, now);
    }

    // ── Option selection ──────────────────────────────────────────────────────

    private async Task<(string Symbol, decimal Ask)?> FindBestCallOptionAsync(
        string ticker, decimal underlyingPrice, decimal maxAsk, decimal maxSpreadPct, DateTime now)
    {
        var     minExpiry     = DateOnly.FromDateTime(now.Date);
        var     maxExpiry     = DateOnly.FromDateTime(now.Date.AddDays(10));
        decimal strikeFloor   = Math.Round(underlyingPrice * 0.97m, 2);  // up to -3% ITM
        decimal strikeCeiling = Math.Round(underlyingPrice * 1.02m, 2);  // up to +2% OTM

        _logger.LogInformation(
            "ReversalCall [{Ticker}]: call chain request — expiry {Min}–{Max}  strike {Floor:F2}–{Ceil:F2}  maxAsk=${MaxAsk:F2}.",
            ticker, minExpiry, maxExpiry, strikeFloor, strikeCeiling, maxAsk);

        IDictionaryPage<IOptionSnapshot> chainPage;
        try
        {
            var req = new OptionChainRequest(ticker)
            {
                ExpirationDateGreaterThanOrEqualTo = minExpiry,
                ExpirationDateLessThanOrEqualTo    = maxExpiry,
                OptionType                         = OptionType.Call,
                StrikePriceGreaterThanOrEqualTo    = strikeFloor,
                StrikePriceLessThanOrEqualTo       = strikeCeiling,
            };
            chainPage = await _optionsDataClient.GetOptionChainAsync(req);
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: chain returned {Count} contract(s) total.", ticker, chainPage.Items.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalCall [{Ticker}]: failed to fetch call chain.", ticker);
            return null;
        }

        if (chainPage.Items.Count == 0)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: empty chain — no calls exist for expiry {Min}–{Max} strike {Floor:F2}–{Ceil:F2}.",
                ticker, minExpiry, maxExpiry, strikeFloor, strikeCeiling);
            return null;
        }

        // Scoring: gamma/ask = most gamma exposure per dollar spent.
        var allCandidates = chainPage.Items
            .Select(kv =>
            {
                decimal ask       = kv.Value.Quote?.AskPrice ?? 0m;
                decimal bid       = kv.Value.Quote?.BidPrice ?? 0m;
                decimal delta     = kv.Value.Greeks?.Delta ?? 0m;
                decimal gamma     = kv.Value.Greeks?.Gamma ?? 0m;
                decimal spreadPct = ask > 0m ? (ask - bid) / ask : 1m;
                decimal score     = ask > 0m ? gamma / ask : 0m;
                bool meetsCore =
                    ask >= 1.00m && ask <= maxAsk      // $1–$10/share = $100–$1 000/contract
                 && delta >= 0.40m && delta <= 0.70m   // near-ATM, real directional exposure
                 && gamma > 0m;
                bool liquid = spreadPct < maxSpreadPct;
                DateOnly expiry = ParseExpiryFromSymbol(kv.Key, now);
                return (Symbol: kv.Key, Ask: ask, Bid: bid, SpreadPct: spreadPct, Delta: delta,
                        Gamma: gamma, Score: score, Expiry: expiry, MeetsCore: meetsCore, Liquid: liquid);
            })
            .OrderBy(x => x.Expiry)
            .ThenByDescending(x => x.Score)
            .ToList();

        var coreCandidates = allCandidates.Where(x => x.MeetsCore).ToList();

        _logger.LogInformation(
            "ReversalCall [{Ticker}]: {Pass}/{Total} contract(s) passed core filters " +
            "(ask $1–${MaxAsk:F2}/share [$100–${MaxCost:F0}/contract], delta 0.40–0.70, gamma > 0).",
            ticker, coreCandidates.Count, chainPage.Items.Count, maxAsk, maxAsk * 100m);

        if (coreCandidates.Count == 0)
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: no contracts met core criteria — maxAsk=${MaxAsk:F2}.", ticker, maxAsk);
            return null;
        }

        var expiries = coreCandidates.Select(x => x.Expiry).Distinct().OrderBy(e => e).ToList();

        foreach (var expiry in expiries)
        {
            var atExpiry       = coreCandidates.Where(x => x.Expiry == expiry).ToList();
            var liquidAtExpiry = atExpiry.Where(x => x.Liquid).ToList();

            if (liquidAtExpiry.Count == 0)
            {
                _logger.LogWarning(
                    "ReversalCall [{Ticker}]: expiry {Expiry} — no liquidity (need spread < {MaxSpread:P0}) " +
                    "among {Count} candidate(s) — trying next expiry.",
                    ticker, expiry, maxSpreadPct, atExpiry.Count);
                continue;
            }

            var best = liquidAtExpiry.OrderByDescending(x => x.Score).First();

            _logger.LogInformation(
                "ReversalCall [{Ticker}]: ★ BEST — {Symbol}  ask=${Ask:F2}  bid=${Bid:F2}  spread={Spread:P1}  " +
                "delta={Delta:F3}  gamma={Gamma:F4}  gamma/ask={Score:F4}  expiry={Expiry}  " +
                "({Count} liquid candidate(s) at this expiry).",
                ticker, best.Symbol, best.Ask, best.Bid, best.SpreadPct, best.Delta, best.Gamma, best.Score,
                best.Expiry, liquidAtExpiry.Count);

            return (best.Symbol, best.Ask);
        }

        _logger.LogWarning(
            "ReversalCall [{Ticker}]: SKIP — no liquid contract found at any expiry {Min}–{Max} (spread < {MaxSpread:P0}).",
            ticker, minExpiry, maxExpiry, maxSpreadPct);
        return null;
    }

    private static DateOnly ParseExpiryFromSymbol(string occSymbol, DateTime now)
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
        catch { /* fall through */ }
        return DateOnly.FromDateTime(now.Date);
    }

    // Parses the strike price from an OCC option symbol, e.g. "TSLA250117C00250000" → 250.00
    private static decimal? ParseStrikeFromSymbol(string occSymbol)
    {
        try
        {
            int i = 0;
            while (i < occSymbol.Length && !char.IsDigit(occSymbol[i])) i++;
            int strikeStart = i + 7;
            if (strikeStart + 8 <= occSymbol.Length)
            {
                string strikeDigits = occSymbol.Substring(strikeStart, 8);
                if (long.TryParse(strikeDigits, out long raw))
                    return raw / 1000m;
            }
        }
        catch { /* fall through */ }
        return null;
    }

    // ── Latest option quote (for exit checks) ────────────────────────────────

    private async Task<(decimal Bid, decimal Ask)?> GetOptionQuoteAsync(string optionSymbol, string ticker)
    {
        try
        {
            var req    = new LatestOptionsDataRequest(new[] { optionSymbol });
            var quotes = await _optionsDataClient.ListLatestQuotesAsync(req);
            if (quotes.TryGetValue(optionSymbol, out var q))
                return (q.BidPrice, q.AskPrice);

            _logger.LogWarning(
                "ReversalCall [{Ticker}]: no latest quote returned for {Symbol}.", ticker, optionSymbol);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalCall [{Ticker}]: failed to fetch latest quote for {Symbol}.", ticker, optionSymbol);
            return null;
        }
    }

    // ── Order helpers ─────────────────────────────────────────────────────────

    private async Task PlaceCallBuyOrderAsync(
        string ticker, string optionSymbol, decimal currentPrice, decimal premium, DateTime now)
    {
        try
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: submitting BUY order — symbol={Symbol}  qty=1  type=Market  tif=Day.",
                ticker, optionSymbol);

            var req   = new NewOrderRequest(optionSymbol, 1, OrderSide.Buy, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "ReversalCall [{Ticker}]: ✔ BUY order accepted — orderId={OrderId}  symbol={Symbol}  status={Status}  " +
                "underlying={Price:F2}  premium(ask)={Premium:F2}  " +
                "exit on -15% stop loss or trailing stop (arms at +15%, trails 5% below peak).",
                ticker, order.OrderId, optionSymbol, order.OrderStatus, currentPrice, premium);

            await _positionStore.OpenAsync(ticker, optionSymbol, premium, currentPrice, now);

            decimal? strike = ParseStrikeFromSymbol(optionSymbol);

            await SendAlertAsync(ticker, optionSymbol, currentPrice, "ENTRY", now, isEntry: true,
                strike: strike, premium: premium);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalCall [{Ticker}]: ✘ FAILED to place buy order for {Symbol}.", ticker, optionSymbol);
        }
    }

    private async Task ClosePositionAsync(
        string ticker, ReversalPositionStore.Position pos, DateTime now, string reason, decimal exitPremium)
    {
        try
        {
            _logger.LogInformation(
                "ReversalCall [{Ticker}]: placing SELL order — symbol={Symbol}  reason={Reason}.",
                ticker, pos.OptionSymbol, reason);

            var req   = new NewOrderRequest(pos.OptionSymbol, 1, OrderSide.Sell, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "ReversalCall [{Ticker}]: ✔ close order accepted — orderId={OrderId}  status={Status}  " +
                "entryPremium=${Entry:F2}  exitPremium=${Exit:F2}.",
                ticker, order.OrderId, order.OrderStatus, pos.EntryPremium, exitPremium);

            await SendAlertAsync(ticker, pos.OptionSymbol, pos.EntryUnderlying, reason, now, isEntry: false,
                entryPremium: pos.EntryPremium, exitPremium: exitPremium);

            decimal pnlDollars  = (exitPremium - pos.EntryPremium) * 100m;
            bool    openedToday = pos.EntryTime.Date == now.Date;
            await _riskState.RecordTradeClosedAsync(DateOnly.FromDateTime(now), pnlDollars, openedToday, "ReversalCall");

            await _positionStore.CloseAsync(ticker);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "ReversalCall [{Ticker}]: ✘ FAILED to close position {Symbol} — position may still be open!",
                ticker, pos.OptionSymbol);
        }
    }

    // ── Email alerts ──────────────────────────────────────────────────────────

    private async Task SendAlertAsync(
        string ticker, string optionSymbol, decimal underlyingPrice,
        string reason, DateTime now, bool isEntry,
        decimal? strike = null, decimal? premium = null,
        decimal? entryPremium = null, decimal? exitPremium = null)
    {
        string sender     = Environment.GetEnvironmentVariable("GMAIL_SENDER")     ?? "";
        string password   = Environment.GetEnvironmentVariable("GMAIL_PASSWORD")   ?? "";
        string recipients = Environment.GetEnvironmentVariable("ALERT_RECIPIENTS") ?? "";

        if (string.IsNullOrEmpty(sender) || string.IsNullOrEmpty(recipients))
        {
            _logger.LogWarning(
                "ReversalCall [{Ticker}]: email not configured (GMAIL_SENDER/ALERT_RECIPIENTS missing) — skipping alert.",
                ticker);
            return;
        }

        string action = isEntry ? "Call Entry (Bullish Reversal)" : $"Call Closed — {reason}";
        string color  = isEntry ? "#1a7a1a" : (reason.StartsWith("STOP") ? "#c0392b" : "#1a7a1a");

        decimal? pnlPct = (!isEntry && entryPremium.HasValue && exitPremium.HasValue && entryPremium.Value != 0m)
            ? (exitPremium.Value - entryPremium.Value) / entryPremium.Value
            : null;

        string body = $"""
            <html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">
            <h2 style="color:{color}">ReversalCall — {ticker} {action}</h2>
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
                  <tr><td style="padding:8px 16px;color:#555">Exit Plan</td>
                      <td style="padding:8px 16px;color:#c0392b;font-weight:bold">
                        Stop loss at -{StopLossPct:P0} premium · trailing stop arms at +{TrailingActivatePct:P0}, trails {TrailingStepPct:P0} below peak · EOD force-close {EodForceCloseTimeOfDay} ET
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
              Qty: 1 contract · Entry: Market · Exit: Market · Day ·
              Scan: every 5 min (entry checked on 15-min bar close), 9:45–15:55 ET ·
              Pattern: hammer/dragonfly-doji reversal on latest 15-min bar with
              downtrend + RSI oversold + support + volume confirmation ·
              Risk cap: {MaxRiskPct:P0} of cash · Max spread: {MaxSpreadPct:P0} ·
              Stop loss: -{StopLossPct:P0} · Trailing stop arms at +{TrailingActivatePct:P0}, trails {TrailingStepPct:P0}
            </p>
            </body></html>
            """;

        using var msg = new MailMessage();
        msg.From       = new MailAddress(sender);
        msg.Subject    = $"ReversalCall {ticker} {action} — {optionSymbol} @ {now:HH:mm} ET";
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
                "ReversalCall [{Ticker}]: email alert sent — action={Action}  recipients={Recipients}.",
                ticker, action, recipients);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalCall [{Ticker}]: failed to send alert email.", ticker);
        }
    }
}
