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
/// WallBounceFunction — GEX put-wall bounce strategy.
///
/// Concept:
///   A "put wall" is a strike where dealers have sold large quantities of puts.
///   To hedge, they must buy the underlying as price falls toward that strike.
///   This mechanical buying creates real, predictable support:
///
///     Price approaches put wall →
///       Dealer delta-hedging generates buy flow →
///       Price bounces / stabilises at the wall →
///       Long call profits from the bounce.
///
///   Critical risk:
///     If price CLOSES BELOW the put wall, the dynamic inverts — dealers who
///     were buyers are now forced to sell (their delta flips). What was support
///     becomes fuel for acceleration downward. Exit IMMEDIATELY on a wall break.
///
/// Entry (every 1 minute):
///   1. Fetch full option chain (calls + puts, 0-30 DTE) for GEX analysis.
///   2. Identify nearest significant put wall at or below current price.
///   3. SKIP if price is more than 1% above the put wall
///      (too far away — bounce may have already happened or wall is irrelevant).
///   4. SKIP if the latest completed 1-min bar is NOT bullish (close ≤ open)
///      — must see actual buying pressure before entering.
///   5. Buy 1 near-ATM call (delta 0.40–0.70, $1–$10/share, 0–10 DTE).
///      Capture the put-wall level for the exit stop.
///
/// Exit (same 1-minute timer):
///   • Wall broken    — exit immediately if latest 1-min bar CLOSES below the
///                      put wall captured at entry (dealer support has flipped).
///   • Stop loss      — exit if premium falls 20% from entry (hard floor).
///   • Trailing stop  — arms once premium gains ≥ 5% from entry;
///                      thereafter close if premium pulls back 2% from its peak.
///   • EOD            — force-close at 15:50 ET regardless (no overnight risk).
/// </summary>
public class WallBounceFunction
{
    private static readonly TimeZoneInfo ET =
        TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

    // ── Tuneable parameters ───────────────────────────────────────────────────
    // Identical sizing to ReversalCall and GexFunction:
    //   $2 000 account · 50% risk per trade = $1 000 max contract cost
    //   Max ask $10/share, min ask $1/share ($100 contract minimum)
    //   Delta 0.40–0.70 · strike −3% to +2% · 0–10 DTE
    //
    // Trailing stop — "2% below current option price":
    //   Each 1-min tick, trail level = currentPremium × 0.98.
    //   The trail level is a HIGH-WATER MARK: it only ever moves UP as the
    //   option price rises (never decreases). This means the stop always sits
    //   2% below the best price seen since the trailing stop armed.
    //   Close when currentPremium drops below trailLevel.
    private const decimal MaxRiskPct          = 1.00m;  // 100% of tradable cash
    private const decimal MaxSpreadPct        = 0.10m;  // max bid/ask spread as % of ask
    private const decimal PutWallProximityPct = 0.01m;  // entry only if price within 1% of put wall
    private const decimal StopLossPct         = 0.20m;  // hard stop: exit if premium down 20% from entry
    private const decimal TrailingActivatePct = 0.05m;  // trailing stop arms at +5% premium profit
    private const decimal TrailingStepPct     = 0.02m;  // trail level = currentPremium × (1 − 0.02)

    private static readonly TimeSpan EodForceCloseTime = new(15, 50, 0);

    private readonly IAlpacaTradingClient          _tradingClient;
    private readonly IAlpacaDataClient             _dataClient;
    private readonly IAlpacaOptionsDataClient      _optionsDataClient;
    private readonly IConfiguration                _config;
    private readonly ILogger<WallBounceFunction>   _logger;
    private readonly WallBouncePositionStore       _positionStore;
    private readonly RiskState                     _riskState;
    // Cross-strategy coordination
    private readonly GexPositionStore              _gexStore;

    public WallBounceFunction(
        IAlpacaTradingClient tradingClient,
        IAlpacaDataClient dataClient,
        IAlpacaOptionsDataClient optionsDataClient,
        IConfiguration config,
        ILogger<WallBounceFunction> logger,
        WallBouncePositionStore positionStore,
        RiskState riskState,
        GexPositionStore gexStore)
    {
        _tradingClient     = tradingClient;
        _dataClient        = dataClient;
        _optionsDataClient = optionsDataClient;
        _config            = config;
        _logger            = logger;
        _positionStore     = positionStore;
        _riskState         = riskState;
        _gexStore          = gexStore;
    }

    // ── Main timer — every 1 minute ──────────────────────────────────────────

    [Function("WallBounce")]
    public async Task Run([TimerTrigger("20 * * * * *", UseMonitor = false)] TimerInfo timer)
    {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, ET);

        if (!IsActiveSession(now)) return;

        foreach (var ticker in Tickers.GexWatchlist)
        {
            try
            {
                await ProcessTickerAsync(ticker, now);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WallBounce [{Ticker}]: unhandled error — skipping ticker.", ticker);
            }
        }
    }

    // ── Session guard ─────────────────────────────────────────────────────────

    private bool IsActiveSession(DateTime now)
    {
        if (now.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
            return false;

        if (MarketCalendar.IsHoliday(now))
            return false;

        var open  = now.Date.AddHours(9).AddMinutes(45);
        var close = now.Date.AddHours(15).AddMinutes(55);

        return now >= open && now <= close;
    }

    // ── Per-ticker: exit check first, then entry scan ─────────────────────────

    private async Task ProcessTickerAsync(string ticker, DateTime now)
    {
        var pos = await _positionStore.TryGetAsync(ticker);

        if (pos is not null)
        {
            await CheckExitAsync(ticker, pos, now);
            return; // one position per ticker at a time
        }

        await TryEnterAsync(ticker, now);
    }

    // ── Exit ──────────────────────────────────────────────────────────────────

    private async Task CheckExitAsync(string ticker, WallBouncePositionStore.Position pos, DateTime now)
    {
        // ── EOD force-close ──────────────────────────────────────────────────
        if (now.TimeOfDay >= EodForceCloseTime)
        {
            var eodQ = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
            decimal eodExit = eodQ?.Bid ?? pos.EntryPremium;
            _logger.LogInformation(
                "WallBounce [{Ticker}]: EOD FORCE CLOSE at {Time} ET.", ticker, now.ToString("HH:mm"));
            await ClosePositionAsync(ticker, pos, now, "EOD FORCE CLOSE", eodExit);
            return;
        }

        // ── Get latest 1-min bar close for wall-break check ──────────────────
        var bars = await FetchRecentBarsAsync(ticker, minutes: 3);
        decimal latestClose = bars is { Count: > 0 } ? (decimal)bars[^1].Close : 0m;

        // ── Wall-break stop — highest priority exit ───────────────────────────
        // If the last 1-min bar closed BELOW the put wall, dealer support has
        // flipped: they are now delta-selling, accelerating the move down.
        // Exit immediately — do not wait for the next premium check.
        if (latestClose > 0m && latestClose < pos.PutWallLevel)
        {
            var wbQ = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
            decimal wbExit = wbQ?.Bid ?? pos.EntryPremium;
            _logger.LogWarning(
                "WallBounce [{Ticker}]: EXIT — PUT WALL BROKEN. price close ${Close:F2} < wall ${Wall:F2}. " +
                "Dealer support has inverted — exiting immediately. premium=${Exit:F2}.",
                ticker, latestClose, pos.PutWallLevel, wbExit);
            await ClosePositionAsync(ticker, pos, now, $"PUT WALL BROKEN (close ${latestClose:F2} < wall ${pos.PutWallLevel:F2})", wbExit);
            return;
        }

        // ── Premium-based exits ───────────────────────────────────────────────
        var quote = await GetOptionQuoteAsync(pos.OptionSymbol, ticker);
        if (quote is null)
        {
            _logger.LogWarning(
                "WallBounce [{Ticker}]: could not fetch quote for {Symbol} — skipping exit check.", ticker, pos.OptionSymbol);
            return;
        }

        decimal currentPremium = quote.Value.Bid;
        if (currentPremium <= 0m)
        {
            _logger.LogWarning(
                "WallBounce [{Ticker}]: bid is ${Bid:F2} for {Symbol} — skipping.", ticker, currentPremium, pos.OptionSymbol);
            return;
        }

        decimal pnlPct = (currentPremium - pos.EntryPremium) / pos.EntryPremium;

        // ── Hard stop loss — 20% below entry ─────────────────────────────────
        if (pnlPct <= -StopLossPct)
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: EXIT — STOP LOSS. premium ${Current:F2} ({Pnl:P1}) vs entry ${Entry:F2}.",
                ticker, currentPremium, pnlPct, pos.EntryPremium);
            await ClosePositionAsync(ticker, pos, now, $"STOP LOSS (-{StopLossPct:P0} premium)", currentPremium);
            return;
        }

        // ── Arm trailing stop at +5% ──────────────────────────────────────────
        if (!pos.TrailingActive && pnlPct >= TrailingActivatePct)
        {
            pos.TrailingActive = true;
            _logger.LogInformation(
                "WallBounce [{Ticker}]: trailing stop ARMED — premium ${Current:F2} ({Pnl:P1}) ≥ +{Activate:P0}. " +
                "Trail will sit 2% below current option price each tick.",
                ticker, currentPremium, pnlPct, TrailingActivatePct);
        }

        // ── Trailing stop — high-water mark 2% below current option price ─────
        // Each tick:
        //   trailLevel = currentPremium × 0.98
        //   PeakPremium stores the HIGH-WATER MARK of that trail level
        //   (i.e. the highest trail level ever seen = best protection locked in).
        //   Close when currentPremium < PeakPremium (the stored trail high-water).
        if (pos.TrailingActive)
        {
            decimal thisTickTrail = currentPremium * (1m - TrailingStepPct);

            // Ratchet the high-water mark upward only
            if (thisTickTrail > pos.PeakPremium)
                pos.PeakPremium = thisTickTrail;

            // Persist updated trail high-water mark
            await _positionStore.SaveAsync(ticker, pos);

            if (currentPremium < pos.PeakPremium)
            {
                _logger.LogInformation(
                    "WallBounce [{Ticker}]: EXIT — TRAILING STOP. premium ${Current:F2} < trail high-water ${Trail:F2} " +
                    "(trail was 2% below best price seen). pnl={Pnl:P1}.",
                    ticker, currentPremium, pos.PeakPremium, pnlPct);
                await ClosePositionAsync(ticker, pos, now,
                    $"TRAILING STOP (price ${currentPremium:F2} < trail ${pos.PeakPremium:F2})", currentPremium);
                return;
            }

            _logger.LogInformation(
                "WallBounce [{Ticker}]: holding {Symbol} — premium=${Current:F2} ({Pnl:P1})  " +
                "trailHighWater=${Trail:F2}  putWall=${Wall:F2}.",
                ticker, pos.OptionSymbol, currentPremium, pnlPct,
                pos.PeakPremium, pos.PutWallLevel);
        }
        else
        {
            // Trailing not yet armed — persist nothing, just log
            _logger.LogInformation(
                "WallBounce [{Ticker}]: holding {Symbol} — premium=${Current:F2} ({Pnl:P1})  " +
                "putWall=${Wall:F2}  trailing arms at +{Activate:P0}.",
                ticker, pos.OptionSymbol, currentPremium, pnlPct,
                pos.PutWallLevel, TrailingActivatePct);
        }
    }

    // ── Entry ─────────────────────────────────────────────────────────────────

    private async Task TryEnterAsync(string ticker, DateTime now)
    {
        // ── Risk circuit breaker ─────────────────────────────────────────────
        var today = DateOnly.FromDateTime(now);
        if (await _riskState.IsHaltedAsync(today))
        {
            _logger.LogWarning(
                "WallBounce [{Ticker}]: SKIP — risk halted: {Reason}.",
                ticker, await _riskState.GetHaltReasonAsync(today));
            return;
        }

        // ── Cross-strategy coordination ──────────────────────────────────────
        if (await _gexStore.HasOpenPositionAsync(ticker))
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: SKIP entry — GexFunction already has an open position on this ticker.", ticker);
            return;
        }

        // ── Fetch 1-min bars — need both price and bullish confirmation ───────
        var bars = await FetchRecentBarsAsync(ticker, minutes: 3);
        if (bars is null || bars.Count == 0)
        {
            _logger.LogWarning("WallBounce [{Ticker}]: SKIP — no recent bars.", ticker);
            return;
        }

        var    lastBar       = bars[^1];
        decimal latestClose  = (decimal)lastBar.Close;
        decimal lastBarOpen  = (decimal)lastBar.Open;
        bool    isBullish    = latestClose > lastBarOpen;

        // ── Gate 1: last 1-min bar must be bullish ────────────────────────────
        if (!isBullish)
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: SKIP — last 1-min bar bearish (open=${Open:F2} close=${Close:F2}). " +
                "Need bullish confirmation before entering near put wall.",
                ticker, lastBarOpen, latestClose);
            return;
        }

        // ── Fetch full chain for GEX (calls + puts) ──────────────────────────
        var chain = await FetchFullChainAsync(ticker, now);
        if (chain is null) return;

        // ── GEX analysis ──────────────────────────────────────────────────────
        var gex = GexAnalyzer.Analyse(chain.Items, latestClose);

        _logger.LogInformation(
            "WallBounce [{Ticker}]: price=${Price:F2}  putWall=${Wall:F2}  proximity={Prox:P2}  " +
            "bullish1min=✔ (open=${Open:F2} close=${Close:F2}).",
            ticker, latestClose, gex.NearestPutWallBelow, gex.PutWallProximity, lastBarOpen, latestClose);

        // ── Gate 2: no meaningful put wall found ──────────────────────────────
        if (gex.NearestPutWallBelow <= 0m)
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: SKIP — no significant put wall identified in chain.", ticker);
            return;
        }

        // ── Gate 3: price must be within 1% above the put wall ───────────────
        // Too far above = the bounce has already happened or the wall is not
        // providing active support pressure right now.
        if (gex.PutWallProximity > PutWallProximityPct)
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: SKIP — price ${Price:F2} is {Prox:P2} above put wall ${Wall:F2} " +
                "(threshold {Thresh:P0}).",
                ticker, latestClose, gex.PutWallProximity, gex.NearestPutWallBelow, PutWallProximityPct);
            return;
        }

        // ── Account / cash gates ──────────────────────────────────────────────
        var positions = await _tradingClient.ListPositionsAsync();
        bool alreadyIn = positions.Any(p =>
            p.Symbol.StartsWith(ticker, StringComparison.OrdinalIgnoreCase)
         && p.Symbol.Length > ticker.Length + 2);

        if (alreadyIn)
        {
            _logger.LogInformation("WallBounce [{Ticker}]: SKIP — Alpaca already shows an option position.", ticker);
            return;
        }

        var     account = await _tradingClient.GetAccountAsync();
        decimal cash    = account.TradableCash;

        await _riskState.EnsureDailyStartCashAsync(today, cash);

        if (cash <= 0m)
        {
            _logger.LogWarning("WallBounce [{Ticker}]: SKIP — no tradable cash.", ticker);
            return;
        }

        decimal maxRiskPct = (decimal)_config.GetValue<double>("WB_MAX_RISK_PCT", (double)MaxRiskPct);
        decimal riskBudget = cash * maxRiskPct;
        decimal maxAsk     = Math.Floor(riskBudget / 100m * 100m) / 100m; // round down to per-share limit

        _logger.LogInformation(
            "WallBounce [{Ticker}]: cash=${Cash:F2}  budget={Pct:P0}×cash=${Budget:F2}  maxAsk=${MaxAsk:F2}/share.",
            ticker, cash, maxRiskPct, riskBudget, maxAsk);

        if (maxAsk < 1.00m)
        {
            _logger.LogWarning(
                "WallBounce [{Ticker}]: SKIP — budget ${Budget:F2} too low (maxAsk=${MaxAsk:F2}/share, need ≥ $1).",
                ticker, riskBudget, maxAsk);
            return;
        }

        // ── Select best call ──────────────────────────────────────────────────
        var best = SelectBestCall(ticker, chain.Items, latestClose, maxAsk, now);
        if (best is null) return;

        if (cash < best.Value.Ask * 100m)
        {
            _logger.LogWarning(
                "WallBounce [{Ticker}]: SKIP — insufficient cash ${Cash:F2} < contract cost ${Cost:F2}.",
                ticker, cash, best.Value.Ask * 100m);
            return;
        }

        await PlaceBuyOrderAsync(ticker, best.Value.Symbol, latestClose, best.Value.Ask, now, gex.NearestPutWallBelow);
    }

    // ── Chain fetch ───────────────────────────────────────────────────────────

    private async Task<IDictionaryPage<IOptionSnapshot>?> FetchFullChainAsync(string ticker, DateTime now)
    {
        var minExpiry = DateOnly.FromDateTime(now.Date);
        var maxExpiry = DateOnly.FromDateTime(now.Date.AddDays(30));

        try
        {
            var req = new OptionChainRequest(ticker)
            {
                ExpirationDateGreaterThanOrEqualTo = minExpiry,
                ExpirationDateLessThanOrEqualTo    = maxExpiry,
                // No OptionType filter — need both calls and puts for GEX put-wall detection
            };

            var page = await _optionsDataClient.GetOptionChainAsync(req);

            if (page.Items.Count == 0)
            {
                _logger.LogWarning("WallBounce [{Ticker}]: empty chain.", ticker);
                return null;
            }

            return page;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WallBounce [{Ticker}]: failed to fetch chain.", ticker);
            return null;
        }
    }

    // ── Call selection ────────────────────────────────────────────────────────

    private (string Symbol, decimal Ask)? SelectBestCall(
        string ticker,
        IReadOnlyDictionary<string, IOptionSnapshot> chain,
        decimal underlyingPrice, decimal maxAsk, DateTime now)
    {
        // Near-ATM calls: delta 0.40–0.70, $1–maxAsk/share, 0–10 DTE, -3% to +2% OTM.
        // We're buying into a bounce setup — want real directional exposure, not lottery tickets.
        var minExpiry   = DateOnly.FromDateTime(now.Date.AddDays(1));  // exclude 0 DTE — theta too destructive
        var maxExpiry   = DateOnly.FromDateTime(now.Date.AddDays(10));
        decimal floor   = Math.Round(underlyingPrice * 0.97m, 2);  // slightly ITM allowed
        decimal ceiling = Math.Round(underlyingPrice * 1.02m, 2);  // up to +2% OTM

        var candidates = chain
            .Where(kv => GexAnalyzer.IsCall(kv.Key))
            .Select(kv =>
            {
                decimal ask       = kv.Value.Quote?.AskPrice ?? 0m;
                decimal bid       = kv.Value.Quote?.BidPrice ?? 0m;
                decimal delta     = kv.Value.Greeks?.Delta ?? 0m;
                decimal gamma     = kv.Value.Greeks?.Gamma ?? 0m;
                decimal spreadPct = ask > 0m ? (ask - bid) / ask : 1m;
                decimal score     = ask > 0m ? gamma / ask : 0m;
                DateOnly expiry   = ParseExpiry(kv.Key, now);
                decimal strike    = GexAnalyzer.ParseStrike(kv.Key);

                bool meetsCore =
                    ask >= 1.00m && ask <= maxAsk
                 && delta >= 0.40m && delta <= 0.70m
                 && gamma > 0m
                 && expiry >= minExpiry                 // exclude 0 DTE
                 && expiry <= maxExpiry                 // 1–10 DTE
                 && strike >= floor && strike <= ceiling;

                bool liquid = spreadPct < MaxSpreadPct;

                return (Symbol: kv.Key, Ask: ask, Bid: bid, SpreadPct: spreadPct,
                        Delta: delta, Gamma: gamma, Score: score, Strike: strike,
                        Expiry: expiry, MeetsCore: meetsCore, Liquid: liquid);
            })
            .OrderBy(x => x.Expiry)
            .ThenByDescending(x => x.Score)
            .ToList();

        var core = candidates.Where(x => x.MeetsCore).ToList();

        _logger.LogInformation(
            "WallBounce [{Ticker}]: call selection — {Pass}/{Total} passed " +
            "(ask $1–${Max:F2}/share, delta 0.40–0.70, ≤ {Expiry}, strike {Floor:F2}–{Ceil:F2}).",
            ticker, core.Count, candidates.Count, maxAsk, maxExpiry, floor, ceiling);

        if (core.Count == 0)
        {
            _logger.LogWarning("WallBounce [{Ticker}]: no calls met criteria.", ticker);
            return null;
        }

        foreach (var expiry in core.Select(x => x.Expiry).Distinct().OrderBy(e => e))
        {
            var liquid = core.Where(x => x.Expiry == expiry && x.Liquid).ToList();

            if (liquid.Count == 0)
            {
                _logger.LogWarning(
                    "WallBounce [{Ticker}]: expiry {Expiry} — no liquid calls (spread < {Max:P0}) — trying next.",
                    ticker, expiry, MaxSpreadPct);
                continue;
            }

            var best = liquid.OrderByDescending(x => x.Score).First();

            _logger.LogInformation(
                "WallBounce [{Ticker}]: ★ BEST — {Symbol}  strike=${Strike:F2}  ask=${Ask:F2}  " +
                "delta={Delta:F3}  gamma={Gamma:F4}  spread={Spread:P1}  expiry={Expiry}.",
                ticker, best.Symbol, best.Strike, best.Ask,
                best.Delta, best.Gamma, best.SpreadPct, best.Expiry);

            return (best.Symbol, best.Ask);
        }

        _logger.LogWarning("WallBounce [{Ticker}]: no liquid call found within 10 DTE.", ticker);
        return null;
    }

    // ── Data helpers ──────────────────────────────────────────────────────────

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
            _logger.LogError(ex, "WallBounce [{Ticker}]: failed to fetch 1-min bars.", ticker);
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
            _logger.LogError(ex, "WallBounce [{Ticker}]: failed to fetch quote for {Symbol}.", ticker, optionSymbol);
        }
        return null;
    }

    // ── Order helpers ─────────────────────────────────────────────────────────

    private async Task PlaceBuyOrderAsync(
        string ticker, string optionSymbol, decimal underlyingPrice,
        decimal premium, DateTime now, decimal putWallLevel)
    {
        try
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: submitting BUY — {Symbol}  qty=1  Market  Day  putWall=${Wall:F2}.",
                ticker, optionSymbol, putWallLevel);

            var req   = new NewOrderRequest(optionSymbol, 1, OrderSide.Buy, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "WallBounce [{Ticker}]: ✔ BUY — orderId={Id}  status={Status}  " +
                "underlying=${Price:F2}  premium=${Premium:F2}  putWall=${Wall:F2}  " +
                "exit: wall-break·-{Stop:P0} stop·trailing({Arm:P0}/−{Step:P0})·EOD.",
                ticker, order.OrderId, order.OrderStatus,
                underlyingPrice, premium, putWallLevel,
                StopLossPct, TrailingActivatePct, TrailingStepPct);

            await _positionStore.OpenAsync(ticker, optionSymbol, premium, underlyingPrice, now, putWallLevel);

            await SendAlertAsync(ticker, optionSymbol, underlyingPrice, "ENTRY", now, isEntry: true,
                premium: premium, putWallLevel: putWallLevel);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WallBounce [{Ticker}]: ✘ FAILED to place buy for {Symbol}.", ticker, optionSymbol);
        }
    }

    private async Task ClosePositionAsync(
        string ticker, WallBouncePositionStore.Position pos, DateTime now, string reason, decimal exitPremium)
    {
        try
        {
            _logger.LogInformation(
                "WallBounce [{Ticker}]: placing SELL — {Symbol}  reason={Reason}.", ticker, pos.OptionSymbol, reason);

            var req   = new NewOrderRequest(pos.OptionSymbol, 1, OrderSide.Sell, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "WallBounce [{Ticker}]: ✔ SELL — orderId={Id}  status={Status}  " +
                "entry=${Entry:F2}  exit=${Exit:F2}.",
                ticker, order.OrderId, order.OrderStatus, pos.EntryPremium, exitPremium);

            await SendAlertAsync(ticker, pos.OptionSymbol, pos.EntryUnderlying, reason, now, isEntry: false,
                entryPremium: pos.EntryPremium, exitPremium: exitPremium);

            decimal pnlDollars  = (exitPremium - pos.EntryPremium) * 100m;
            bool    openedToday = pos.EntryTime.Date == now.Date;
            await _riskState.RecordTradeClosedAsync(DateOnly.FromDateTime(now), pnlDollars, openedToday, "WallBounce");

            await _positionStore.CloseAsync(ticker);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "WallBounce [{Ticker}]: ✘ FAILED to close {Symbol} — position may still be open!",
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

    // ── Email alerts ──────────────────────────────────────────────────────────

    private async Task SendAlertAsync(
        string ticker, string optionSymbol, decimal underlyingPrice,
        string reason, DateTime now, bool isEntry,
        decimal? premium = null, decimal putWallLevel = 0m,
        decimal? entryPremium = null, decimal? exitPremium = null)
    {
        string sender     = Environment.GetEnvironmentVariable("GMAIL_SENDER")     ?? "";
        string password   = Environment.GetEnvironmentVariable("GMAIL_PASSWORD")   ?? "";
        string recipients = Environment.GetEnvironmentVariable("ALERT_RECIPIENTS") ?? "";

        if (string.IsNullOrEmpty(sender) || string.IsNullOrEmpty(recipients))
            return;

        string action = isEntry ? "Call Entry (Put Wall Bounce)" : $"Call Closed — {reason}";
        string color  = isEntry ? "#0d6efd"
                      : reason.StartsWith("PUT WALL") ? "#c0392b"
                      : reason.StartsWith("STOP")     ? "#c0392b"
                      : reason.StartsWith("EOD")      ? "#e67e22"
                      : "#1a7a1a";

        decimal? pnlPct = (!isEntry && entryPremium.HasValue && exitPremium.HasValue && entryPremium.Value != 0m)
            ? (exitPremium.Value - entryPremium.Value) / entryPremium.Value
            : null;

        string body = $"""
            <html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">
            <h2 style="color:{color}">WallBounce — {ticker} {action}</h2>
            <table style="border-collapse:collapse;background:#fff;border-radius:8px;padding:16px;min-width:360px">
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
                  <tr><td style="padding:8px 16px;color:#555">Premium (Ask)</td>
                      <td style="padding:8px 16px;font-weight:bold">${premium:F2}  (${premium * 100m:F2}/contract)</td></tr>
                  <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Put Wall Level</td>
                      <td style="padding:8px 16px;font-weight:bold;color:#0d6efd">${putWallLevel:F2}
                        <span style="color:#888;font-size:11px"> — exit immediately if price closes below</span></td></tr>
                  <tr><td style="padding:8px 16px;color:#555">Exit Plan</td>
                      <td style="padding:8px 16px;color:#c0392b;font-weight:bold">
                        Wall break (price &lt; ${putWallLevel:F2}) ·
                        Stop −{StopLossPct:P0} ·
                        Trailing: arms +{TrailingActivatePct:P0}, trails −{TrailingStepPct:P0} from peak ·
                        EOD {EodForceCloseTime} ET
                      </td></tr>
                  """
                : $"""
                  <tr><td style="padding:8px 16px;color:#555">Close Reason</td>
                      <td style="padding:8px 16px;font-weight:bold">{reason}</td></tr>
                  <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Entry → Exit Premium</td>
                      <td style="padding:8px 16px;font-weight:bold">
                        ${entryPremium:F2} → ${exitPremium:F2}
                        {(pnlPct.HasValue ? $"<span style='color:{(pnlPct.Value >= 0 ? "#1a7a1a" : "#c0392b")}'> ({pnlPct.Value:P1})</span>" : "")}
                      </td></tr>
                  """)}
            </table>
            <p style="color:#888;font-size:12px;margin-top:16px">
              WallBounceFunction · GEX put-wall bounce · Qty: 1 contract · Market · Day ·
              Scan: every 1 min, 9:45–15:55 ET · Risk: {MaxRiskPct:P0} of cash ·
              Entry: price within {PutWallProximityPct:P0} of put wall + bullish 1-min bar ·
              Stop: −{StopLossPct:P0} premium · Trailing: +{TrailingActivatePct:P0} arms, −{TrailingStepPct:P0} trail
            </p>
            </body></html>
            """;

        using var msg = new MailMessage();
        msg.From       = new MailAddress(sender);
        msg.Subject    = $"WallBounce {ticker} {action} — {optionSymbol} @ {now:HH:mm} ET";
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
            _logger.LogInformation("WallBounce [{Ticker}]: email sent — {Action}.", ticker, action);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WallBounce [{Ticker}]: failed to send email.", ticker);
        }
    }
}
