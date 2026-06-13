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
/// ElonMoney — fast VOLD-momentum long-call strategy on a 1-minute cadence.
///
/// Concept:
///   Every minute, compute the VOLD ratio (up-volume / down-volume) over a
///   rolling window of the last RollingWindowBars 1-min bars for each
///   watched ticker.
///
///   Entry  — buy 1 call contract when:
///              • the current VOLD ratio is &gt; ELON_VOLD_THRESHOLD (default 1.0), AND
///              • the last 3 VOLD readings are strictly increasing
///                (e.g. 1.1 → 1.2 → 1.3) — confirms building bullish momentum,
///                not just a single-minute blip.
///   Exit   — close the call when the last 3 VOLD readings are strictly
///              decreasing (momentum fading/reversing).
///
///   Liquidity — bid/ask spread must be &lt; ELON_MAX_SPREAD_PCT (default 15%).
///   Sizing    — contract cost capped at ELON_MAX_RISK_PCT (default 50%) of
///               tradable cash — sized for small accounts (e.g. ~$500).
///
/// Runs every minute, 9:45–15:55 ET, Monday–Friday.
/// </summary>
public class ElonMoneyFunction
{
    private static readonly TimeZoneInfo ET =
        TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

    // ── Tuneable parameters ───────────────────────────────────────────────────
    private const int     RollingWindowBars    = 5;      // VOLD computed over the last N 1-min bars
    private const double  VoldThreshold        = 1.0;    // VOLD ratio must be above this to consider entry
    private const decimal MaxSpreadPct         = 0.15m;  // liquidity gate: (ask-bid)/ask must be < 15%
    private const decimal MaxRiskPct           = 0.50m;  // max contract cost as a fraction of tradable cash (50%)

    private readonly IAlpacaTradingClient            _tradingClient;
    private readonly IAlpacaDataClient               _dataClient;
    private readonly IAlpacaOptionsDataClient        _optionsDataClient;
    private readonly IConfiguration                  _config;
    private readonly ILogger<ElonMoneyFunction>      _logger;

    public ElonMoneyFunction(
        IAlpacaTradingClient tradingClient,
        IAlpacaDataClient dataClient,
        IAlpacaOptionsDataClient optionsDataClient,
        IConfiguration config,
        ILogger<ElonMoneyFunction> logger)
    {
        _tradingClient     = tradingClient;
        _dataClient        = dataClient;
        _optionsDataClient = optionsDataClient;
        _config            = config;
        _logger            = logger;
    }

    // ── Entry point — every 1 min ─────────────────────────────────────────────

    [Function("ElonMoney")]
    public async Task Run([TimerTrigger("0 * * * * *")] TimerInfo timer)
    {
        var now = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, ET);
        _logger.LogInformation(
            "ElonMoney: tick at {Time} ET ({DayOfWeek}).", now.ToString("HH:mm:ss"), now.DayOfWeek);

        // ── Weekend guard ─────────────────────────────────────────────────────
        if (now.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
        {
            _logger.LogInformation("ElonMoney: weekend ({Day}) — markets closed, nothing to do.", now.DayOfWeek);
            return;
        }

        // ── Market holiday guard ─────────────────────────────────────────────
        if (MarketCalendar.IsHoliday(now))
        {
            _logger.LogInformation("ElonMoney: {Date} is a market holiday — nothing to do.", now.ToString("yyyy-MM-dd"));
            return;
        }

        // ── Time window — 9:45 to 15:55 ET ───────────────────────────────────
        var sessionOpen  = now.Date.AddHours(9).AddMinutes(45);
        var sessionClose = now.Date.AddHours(15).AddMinutes(55);

        if (now < sessionOpen || now > sessionClose)
        {
            if (now < sessionOpen)
                _logger.LogInformation(
                    "ElonMoney: {Time} ET — too early, session opens at 09:45. Waiting {Min} min.",
                    now.ToString("HH:mm"), (int)(sessionOpen - now).TotalMinutes);
            else
                _logger.LogInformation(
                    "ElonMoney: {Time} ET — past session close (15:55). No action until tomorrow.",
                    now.ToString("HH:mm"));
            return;
        }

        TradingState.ResetIfNewDay(DateOnly.FromDateTime(now), _logger);

        double  voldThreshold = _config.GetValue<double>("ELON_VOLD_THRESHOLD", VoldThreshold);
        decimal maxSpreadPct  = (decimal)_config.GetValue<double>("ELON_MAX_SPREAD_PCT", (double)MaxSpreadPct);
        decimal maxRiskPct    = (decimal)_config.GetValue<double>("ELON_MAX_RISK_PCT", (double)MaxRiskPct);

        var dayStartUtc = DateTime.SpecifyKind(
            TimeZoneInfo.ConvertTimeToUtc(now.Date, ET), DateTimeKind.Utc);
        var nowUtc = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);

        foreach (var ticker in Tickers.Watchlist)
        {
            await ProcessTickerAsync(ticker, now, dayStartUtc, nowUtc, voldThreshold, maxSpreadPct, maxRiskPct);
        }

        _logger.LogInformation("ElonMoney: ── tick complete at {Time} ET ──", now.ToString("HH:mm:ss"));
    }

    private async Task ProcessTickerAsync(
        string ticker, DateTime now, DateTime dayStartUtc, DateTime nowUtc,
        double voldThreshold, decimal maxSpreadPct, decimal maxRiskPct)
    {
        // ── Fetch today's 1-min bars so far ──────────────────────────────────
        var bars = await FetchBarsAsync(ticker, dayStartUtc, nowUtc);
        if (bars.Count < RollingWindowBars)
        {
            _logger.LogInformation(
                "ElonMoney [{Ticker}]: only {Count}/{Needed} bar(s) so far — skipping this tick.",
                ticker, bars.Count, RollingWindowBars);
            return;
        }

        // ── Rolling VOLD ratio over the last N bars ──────────────────────────
        var recentBars = bars.Skip(bars.Count - RollingWindowBars).ToList();
        var vold = VoldCalculator.Calculate(recentBars);
        TradingState.RecordVold(ticker, vold.VoldRatio);

        string ratioStr = vold.VoldRatio == double.PositiveInfinity ? "∞" : vold.VoldRatio.ToString("F4");
        var history = TradingState.GetVoldHistory(ticker);
        string historyStr = string.Join(" → ", history.Select(h => h == double.PositiveInfinity ? "∞" : h.ToString("F2")));

        _logger.LogInformation(
            "ElonMoney [{Ticker}]: VOLD={Ratio}  (rolling {Window}-bar, {Bars} bars today)  history=[{History}]  signal={Signal}.",
            ticker, ratioStr, RollingWindowBars, bars.Count, historyStr, vold.Signal);

        bool hasOpenPosition = TradingState.OpenOptions.ContainsKey(ticker);

        // ── EXIT — 3 consecutive falling VOLD readings ───────────────────────
        if (hasOpenPosition)
        {
            if (TradingState.IsVoldFallingThreeInARow(ticker))
            {
                _logger.LogInformation(
                    "ElonMoney [{Ticker}]: EXIT — VOLD falling 3 ticks in a row [{History}]. Closing call.",
                    ticker, historyStr);
                await ClosePositionAsync(ticker, now);
            }
            else
            {
                _logger.LogInformation(
                    "ElonMoney [{Ticker}]: holding open position — VOLD not falling 3 in a row yet.", ticker);
            }
            return;
        }

        // ── ENTRY — VOLD > threshold AND 3 consecutive rising readings ───────
        if (vold.VoldRatio <= voldThreshold)
        {
            _logger.LogInformation(
                "ElonMoney [{Ticker}]: SKIP — VOLD {Ratio} not above threshold {Threshold}.",
                ticker, ratioStr, voldThreshold);
            return;
        }

        if (!TradingState.IsVoldRisingThreeInARow(ticker))
        {
            _logger.LogInformation(
                "ElonMoney [{Ticker}]: SKIP — VOLD above threshold but not 3 consecutive rises yet [{History}].",
                ticker, historyStr);
            return;
        }

        if (!IsVolumeRisingThreeInARow(bars, out string volumeStr))
        {
            _logger.LogInformation(
                "ElonMoney [{Ticker}]: SKIP — VOLD rising 3x but bar volume not also rising 3x in a row [{Volumes}].",
                ticker, volumeStr);
            return;
        }

        decimal price = (decimal)bars[^1].Close;
        _logger.LogInformation(
            "ElonMoney [{Ticker}]: ★ ENTRY SIGNAL — VOLD {Ratio} > {Threshold} and rising 3 ticks in a row " +
            "[{History}], volume also rising 3 ticks in a row [{Volumes}]  price=${Price:F2}.",
            ticker, ratioStr, voldThreshold, historyStr, volumeStr, price);

        await TryEnterCallAsync(ticker, price, now, maxSpreadPct, maxRiskPct);
    }

    // ── Bar fetch ─────────────────────────────────────────────────────────────

    private async Task<IReadOnlyList<IBar>> FetchBarsAsync(string ticker, DateTime startUtc, DateTime endUtc)
    {
        var request = new HistoricalBarsRequest(
            ticker,
            DateTime.SpecifyKind(startUtc, DateTimeKind.Utc),
            DateTime.SpecifyKind(endUtc,   DateTimeKind.Utc),
            new BarTimeFrame(1, BarTimeFrameUnit.Minute))
        {
            Feed = MarketDataFeed.Sip,
        };

        try
        {
            var page = await _dataClient.ListHistoricalBarsAsync(request);
            _logger.LogDebug("ElonMoney [{Ticker}]: fetched {Count} bar(s).", ticker, page.Items.Count);
            return page.Items;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ElonMoney [{Ticker}]: failed to fetch bars — ticker will be skipped.", ticker);
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
                "ElonMoney [{Ticker}]: SKIP — Alpaca already shows an option position on this ticker.", ticker);
            return;
        }

        // Gate B — account balance / risk cap
        var     account = await _tradingClient.GetAccountAsync();
        decimal cash    = account.TradableCash;

        _logger.LogInformation(
            "ElonMoney [{Ticker}]: account — tradable cash={Cash:C}  buying power={BP:C}.",
            ticker, cash, account.BuyingPower ?? 0m);

        if (cash <= 0m)
        {
            _logger.LogWarning("ElonMoney [{Ticker}]: SKIP — tradable cash is ${Cash:F2}.", ticker, cash);
            return;
        }

        decimal riskBudget = cash * maxRiskPct;
        decimal maxAsk     = Math.Floor(riskBudget / 100m * 100m) / 100m;  // round down to nearest cent

        _logger.LogInformation(
            "ElonMoney [{Ticker}]: risk cap — {RiskPct:P0} of cash ${Cash:F2} = ${Budget:F2} → maxAsk=${MaxAsk:F2} (contract cost ≤ ${Cost:F2}).",
            ticker, maxRiskPct, cash, riskBudget, maxAsk, maxAsk * 100m);

        if (maxAsk < 1.00m)
        {
            _logger.LogWarning(
                "ElonMoney [{Ticker}]: SKIP — {RiskPct:P0} risk budget ${Budget:F2} too low for even a $1.00 ask.",
                ticker, maxRiskPct, riskBudget);
            return;
        }

        // Gate C — find best call option
        var best = await FindBestCallOptionAsync(ticker, currentPrice, maxAsk, maxSpreadPct, now);
        if (best is null)
        {
            _logger.LogWarning(
                "ElonMoney [{Ticker}]: SKIP — no call found meeting delta/liquidity/budget/expiry criteria within ${MaxAsk:F2}.",
                ticker, maxAsk);
            return;
        }

        // Gate D — hard cash check
        decimal contractCost = best.Value.Ask * 100m;
        if (cash < contractCost)
        {
            _logger.LogWarning(
                "ElonMoney [{Ticker}]: SKIP — insufficient cash ${Cash:F2} < contract cost ${Cost:F2}.",
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
        decimal strikeFloor   = Math.Round(underlyingPrice * 0.99m, 2);
        decimal strikeCeiling = Math.Round(underlyingPrice * 1.05m, 2);

        _logger.LogInformation(
            "ElonMoney [{Ticker}]: call chain request — expiry {Min}–{Max}  strike {Floor:F2}–{Ceil:F2}  maxAsk=${MaxAsk:F2}.",
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
                "ElonMoney [{Ticker}]: chain returned {Count} contract(s) total.", ticker, chainPage.Items.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ElonMoney [{Ticker}]: failed to fetch call chain.", ticker);
            return null;
        }

        if (chainPage.Items.Count == 0)
        {
            _logger.LogWarning(
                "ElonMoney [{Ticker}]: empty chain — no calls exist for expiry {Min}–{Max} strike {Floor:F2}–{Ceil:F2}.",
                ticker, minExpiry, maxExpiry, strikeFloor, strikeCeiling);
            return null;
        }

        // Scoring: gamma/ask = most gamma exposure per dollar spent.
        var allCandidates = chainPage.Items
            .Select(kv =>
            {
                decimal ask    = kv.Value.Quote?.AskPrice ?? 0m;
                decimal bid    = kv.Value.Quote?.BidPrice ?? 0m;
                decimal delta  = kv.Value.Greeks?.Delta ?? 0m;
                decimal gamma  = kv.Value.Greeks?.Gamma ?? 0m;
                decimal spread    = ask - bid;
                decimal spreadPct = ask > 0m ? spread / ask : 1m;
                decimal score  = ask > 0m ? gamma / ask : 0m;
                bool meetsCore =
                    ask >= 1.00m
                 && ask <= maxAsk
                 && delta >= 0.30m
                 && delta <= 0.65m
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
            "ElonMoney [{Ticker}]: {Pass}/{Total} contract(s) passed core filters " +
            "(ask $1–${MaxAsk:F2}, delta +0.30–+0.65, gamma > 0).",
            ticker, coreCandidates.Count, chainPage.Items.Count, maxAsk);

        if (coreCandidates.Count == 0)
        {
            _logger.LogWarning(
                "ElonMoney [{Ticker}]: no contracts met core criteria — maxAsk=${MaxAsk:F2}.", ticker, maxAsk);
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
                    "ElonMoney [{Ticker}]: expiry {Expiry} — no liquidity (need spread < {MaxSpread:P0}) " +
                    "among {Count} candidate(s) — trying next expiry.",
                    ticker, expiry, maxSpreadPct, atExpiry.Count);
                continue;
            }

            var best = liquidAtExpiry.OrderByDescending(x => x.Score).First();

            _logger.LogInformation(
                "ElonMoney [{Ticker}]: ★ BEST — {Symbol}  ask=${Ask:F2}  bid=${Bid:F2}  spread={Spread:P1}  " +
                "delta={Delta:F3}  gamma={Gamma:F4}  gamma/ask={Score:F4}  expiry={Expiry}  " +
                "({Count} liquid candidate(s) at this expiry).",
                ticker, best.Symbol, best.Ask, best.Bid, best.SpreadPct, best.Delta, best.Gamma, best.Score,
                best.Expiry, liquidAtExpiry.Count);

            return (best.Symbol, best.Ask);
        }

        _logger.LogWarning(
            "ElonMoney [{Ticker}]: SKIP — no liquid contract found at any expiry {Min}–{Max} (spread < {MaxSpread:P0}).",
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

    // Returns true if the last 3 bars' volumes are strictly increasing
    // (e.g. 5 → 7 → 9). Also outputs a "v1 → v2 → v3" string for logging.
    private static bool IsVolumeRisingThreeInARow(IReadOnlyList<IBar> bars, out string volumeStr)
    {
        int n = bars.Count;
        if (n < 3)
        {
            volumeStr = string.Join(" → ", bars.Select(b => b.Volume.ToString("F0")));
            return false;
        }

        decimal v1 = bars[n - 3].Volume;
        decimal v2 = bars[n - 2].Volume;
        decimal v3 = bars[n - 1].Volume;
        volumeStr = $"{v1:F0} → {v2:F0} → {v3:F0}";

        return v1 < v2 && v2 < v3;
    }

    // ── Order helpers ─────────────────────────────────────────────────────────

    private async Task PlaceCallBuyOrderAsync(
        string ticker, string optionSymbol, decimal currentPrice, decimal premium, DateTime now)
    {
        try
        {
            _logger.LogInformation(
                "ElonMoney [{Ticker}]: submitting BUY order — symbol={Symbol}  qty=1  type=Market  tif=Day.",
                ticker, optionSymbol);

            var req   = new NewOrderRequest(optionSymbol, 1, OrderSide.Buy, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            _logger.LogInformation(
                "ElonMoney [{Ticker}]: ✔ BUY order accepted — orderId={OrderId}  symbol={Symbol}  status={Status}  " +
                "underlying={Price:F2}  premium(ask)={Premium:F2}  " +
                "exit on 3 consecutive falling VOLD readings.",
                ticker, order.OrderId, optionSymbol, order.OrderStatus, currentPrice, premium);

            TradingState.OpenOptions[ticker]   = optionSymbol;
            TradingState.EntryPrices[ticker]   = currentPrice;
            TradingState.EntryPremiums[ticker] = premium;
            TradingState.EntryTimes[ticker]    = now;

            decimal? strike = ParseStrikeFromSymbol(optionSymbol);

            await SendAlertAsync(ticker, optionSymbol, currentPrice, currentPrice, "ENTRY", now, isEntry: true,
                strike: strike, premium: premium);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ElonMoney [{Ticker}]: ✘ FAILED to place buy order for {Symbol}.", ticker, optionSymbol);
        }
    }

    private async Task ClosePositionAsync(string ticker, DateTime now)
    {
        if (!TradingState.OpenOptions.TryGetValue(ticker, out var optionSymbol))
        {
            _logger.LogWarning("ElonMoney [{Ticker}]: no open option symbol on record — cannot close.", ticker);
            return;
        }

        try
        {
            _logger.LogInformation(
                "ElonMoney [{Ticker}]: placing SELL order — symbol={Symbol}  reason=VOLD FALLING 3x.",
                ticker, optionSymbol);

            var req   = new NewOrderRequest(optionSymbol, 1, OrderSide.Sell, OrderType.Market, TimeInForce.Day);
            var order = await _tradingClient.PostOrderAsync(req);

            TradingState.EntryPrices.TryGetValue(ticker, out decimal entryPrice);

            _logger.LogInformation(
                "ElonMoney [{Ticker}]: ✔ close order accepted — orderId={OrderId}  status={Status}.",
                ticker, order.OrderId, order.OrderStatus);

            await SendAlertAsync(ticker, optionSymbol, entryPrice, entryPrice, "VOLD FALLING 3x", now, isEntry: false);

            TradingState.ClearPosition(ticker);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "ElonMoney [{Ticker}]: ✘ FAILED to close position {Symbol} — position may still be open!",
                ticker, optionSymbol);
        }
    }

    // ── Email alerts ──────────────────────────────────────────────────────────

    private async Task SendAlertAsync(
        string ticker, string optionSymbol, decimal price, decimal entryPrice,
        string reason, DateTime now, bool isEntry, decimal? strike = null, decimal? premium = null)
    {
        string sender     = Environment.GetEnvironmentVariable("GMAIL_SENDER")     ?? "";
        string password   = Environment.GetEnvironmentVariable("GMAIL_PASSWORD")   ?? "";
        string recipients = Environment.GetEnvironmentVariable("ALERT_RECIPIENTS") ?? "";

        if (string.IsNullOrEmpty(sender) || string.IsNullOrEmpty(recipients))
        {
            _logger.LogWarning(
                "ElonMoney [{Ticker}]: email not configured (GMAIL_SENDER/ALERT_RECIPIENTS missing) — skipping alert.",
                ticker);
            return;
        }

        string action = isEntry ? "Call Entry (VOLD Momentum)" : $"Call Closed — {reason}";
        string color  = "#1a7a1a";

        string body = $"""
            <html><body style="font-family:Arial,sans-serif;background:#f5f5f5;padding:20px">
            <h2 style="color:{color}">ElonMoney — {ticker} {action}</h2>
            <table style="border-collapse:collapse;background:#fff;border-radius:8px;padding:16px;min-width:340px">
              <tr><td style="padding:8px 16px;color:#555">Ticker</td>
                  <td style="padding:8px 16px;font-weight:bold">{ticker}</td></tr>
              <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Time (ET)</td>
                  <td style="padding:8px 16px;font-weight:bold">{now:HH:mm:ss}</td></tr>
              <tr><td style="padding:8px 16px;color:#555">Option Symbol</td>
                  <td style="padding:8px 16px;font-weight:bold;font-family:monospace">{optionSymbol}</td></tr>
              <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">{ticker} Price</td>
                  <td style="padding:8px 16px;font-weight:bold">${price:F2}</td></tr>
              {(isEntry
                ? $"""
                  <tr><td style="padding:8px 16px;color:#555">Strike</td>
                      <td style="padding:8px 16px;font-weight:bold">{(strike.HasValue ? $"${strike.Value:F2}" : "n/a")}</td></tr>
                  <tr style="background:#f9f9f9"><td style="padding:8px 16px;color:#555">Premium (Ask)</td>
                      <td style="padding:8px 16px;font-weight:bold">{(premium.HasValue ? $"${premium.Value:F2}" : "n/a")}  (cost: {(premium.HasValue ? $"${premium.Value * 100m:F2}" : "n/a")})</td></tr>
                  <tr><td style="padding:8px 16px;color:#555">Exit Condition</td>
                      <td style="padding:8px 16px;color:#c0392b;font-weight:bold">
                        Close when VOLD ratio falls 3 consecutive minutes in a row
                      </td></tr>
                  """
                : $"""
                  <tr><td style="padding:8px 16px;color:#555">Close Reason</td>
                      <td style="padding:8px 16px;font-weight:bold">{reason}</td></tr>
                  """)}
            </table>
            <p style="color:#888;font-size:12px;margin-top:16px">
              Qty: 1 contract · Entry: Market · Exit: Market · Day · " +
              Scan: every 1 min, 9:30–15:55 ET · VOLD threshold &gt; {VoldThreshold} with 3 rising ticks ·
              Risk cap: {MaxRiskPct:P0} of cash · Max spread: {MaxSpreadPct:P0}
            </p>
            </body></html>
            """;

        using var msg = new MailMessage();
        msg.From       = new MailAddress(sender);
        msg.Subject    = $"ElonMoney {ticker} {action} — {optionSymbol} @ {now:HH:mm} ET";
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
                "ElonMoney [{Ticker}]: email alert sent — action={Action}  recipients={Recipients}.",
                ticker, action, recipients);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ElonMoney [{Ticker}]: failed to send alert email.", ticker);
        }
    }
}
