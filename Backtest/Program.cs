using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Alpaca.Markets;

// ── 3-month backtest: TSLA bullish-reversal pattern (PatternDetector) ───────
//
// Scans every 15-min bar over the last ~3 months. Whenever
// PatternDetector.DetectBullishReversal fires (and no position is currently
// open), simulates one trade using the SAME exit rules as ReversalCallFunction:
//   - Stop loss:     premium falls 50% from entry  -> close
//   - Trailing stop: arms once premium is +25% from entry, then closes if
//                     premium pulls back 5% from its running peak
//
// CAVEAT: Alpaca's options-data API has no historical option-quote endpoint,
// so real historical premiums aren't available. Each simulated trade assumes
// a FIXED entry premium ($5.00) and FIXED delta (0.45) — i.e. premium(t) =
// max(0.01, entryPremium + delta * (underlying(t) - underlyingEntry)). This
// is a rough approximation that ignores theta/gamma/IV changes, but is
// reasonable for comparing relative win-rate / frequency of this pattern.

const string Ticker        = "TSLA";
const decimal AssumedEntryPremium = 5.00m;
const decimal AssumedDelta        = 0.45m;
const decimal StopLossPct         = 0.15m;
const decimal TrailingActivatePct = 0.15m;
const decimal TrailingStepPct     = 0.05m;
const int     LookbackDays        = 100; // ~3 months + buffer for the 25-bar warm-up

var et = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");

string apiKey    = Environment.GetEnvironmentVariable("ALPACA_PAPER_API_KEY")
    ?? "PK2W25BK25RYXLKGDNFCWHJTA4";
string apiSecret = Environment.GetEnvironmentVariable("ALPACA_PAPER_SECRET_KEY")
    ?? "E1s6sznhQJufA6SknXJtgmAULbdqtN3xbL82zhyjXQgG";

var secretKey  = new SecretKey(apiKey, apiSecret);
var dataClient = Alpaca.Markets.Environments.Paper.GetAlpacaDataClient(secretKey);

var nowUtc   = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc);
var startUtc = nowUtc.AddDays(-LookbackDays);

Console.WriteLine($"Fetching {Ticker} 15-min bars from {startUtc:yyyy-MM-dd} to {nowUtc:yyyy-MM-dd} (IEX feed)...");

var barsRequest = new HistoricalBarsRequest(
    Ticker, startUtc, nowUtc, new BarTimeFrame(15, BarTimeFrameUnit.Minute))
{
    Feed = MarketDataFeed.Iex,
};

IReadOnlyList<IBar> bars;
try
{
    var page = await dataClient.ListHistoricalBarsAsync(barsRequest);
    bars = page.Items.OrderBy(b => b.TimeUtc).ToList();
    Console.WriteLine($"Fetched {bars.Count} bars.");
}
catch (Exception ex)
{
    Console.WriteLine($"ERROR fetching bars: {ex.Message}");
    return;
}

int minBarsNeeded = PatternDetector.RsiPeriod + PatternDetector.SupportLookbackBars + 1;
if (bars.Count < minBarsNeeded)
{
    Console.WriteLine("Not enough bars to evaluate.");
    return;
}

Console.WriteLine();
Console.WriteLine($"Assumptions: entry premium=${AssumedEntryPremium:F2}, delta={AssumedDelta:F2}, " +
                   $"stop loss=-{StopLossPct:P0}, trailing arm=+{TrailingActivatePct:P0}, trail step={TrailingStepPct:P0}");
Console.WriteLine();
Console.WriteLine($"{"#",3} {"Signal (ET)",-17} {"Entry U",8} {"Exit (ET)",-17} {"Exit U",8} {"Bars",5} {"PnL%",8}  Reason");

var trades = new List<(DateTime SignalTime, decimal EntryU, DateTime ExitTime, decimal ExitU, int BarsHeld, decimal PnlPct, string Reason)>();

int n = bars.Count;
int i = minBarsNeeded - 1;

while (i < n)
{
    var window = bars.Take(i + 1).ToList();
    var result = PatternDetector.DetectBullishReversal(window);

    if (!result.Detected)
    {
        i++;
        continue;
    }

    // Signal fires on bar i. Position would be opened at the close of bar i,
    // then monitored starting from bar i+1.
    var signalBar = bars[i];
    decimal entryU = (decimal)signalBar.Close;
    decimal entryPremium = AssumedEntryPremium;
    decimal peakPremium = entryPremium;
    bool trailingActive = false;

    int j = i + 1;
    decimal exitPremium = entryPremium;
    decimal exitU = entryU;
    DateTime exitTimeUtc = signalBar.TimeUtc;
    string reason = "EOD/no exit (data ended)";

    for (; j < n; j++)
    {
        var b = bars[j];
        decimal underlying = (decimal)b.Close;
        decimal premium = Math.Max(0.01m, entryPremium + AssumedDelta * (underlying - entryU));
        decimal pnlPct = (premium - entryPremium) / entryPremium;

        if (premium > peakPremium) peakPremium = premium;

        if (pnlPct <= -StopLossPct)
        {
            reason = "STOP LOSS (-15%)";
            exitPremium = premium; exitU = underlying; exitTimeUtc = b.TimeUtc;
            break;
        }

        if (!trailingActive && premium >= entryPremium * (1m + TrailingActivatePct))
            trailingActive = true;

        if (trailingActive)
        {
            decimal trailLevel = peakPremium * (1m - TrailingStepPct);
            if (premium <= trailLevel)
            {
                reason = "TRAILING STOP (-5% from peak)";
                exitPremium = premium; exitU = underlying; exitTimeUtc = b.TimeUtc;
                break;
            }
        }

        exitPremium = premium; exitU = underlying; exitTimeUtc = b.TimeUtc;
    }

    decimal finalPnlPct = (exitPremium - entryPremium) / entryPremium;
    var signalEt = TimeZoneInfo.ConvertTimeFromUtc(signalBar.TimeUtc, et);
    var exitEt   = TimeZoneInfo.ConvertTimeFromUtc(exitTimeUtc, et);
    int barsHeld = j - i;

    trades.Add((signalEt, entryU, exitEt, exitU, barsHeld, finalPnlPct, reason));

    Console.WriteLine($"{trades.Count,3} {signalEt:yyyy-MM-dd HH:mm,-17} {entryU,8:F2} {exitEt:yyyy-MM-dd HH:mm,-17} {exitU,8:F2} {barsHeld,5} {finalPnlPct,8:P1}  {reason}");

    // Resume scanning after this trade closes (no overlapping positions).
    i = Math.Max(j, i + 1);
}

Console.WriteLine();
Console.WriteLine("── Summary ──────────────────────────────────────────────");
if (trades.Count == 0)
{
    Console.WriteLine("No signals fired over this period.");
}
else
{
    int wins   = trades.Count(t => t.PnlPct > 0);
    int losses = trades.Count(t => t.PnlPct <= 0);
    decimal avgPnl   = trades.Average(t => t.PnlPct);
    decimal totalPnl = trades.Sum(t => t.PnlPct);
    decimal best     = trades.Max(t => t.PnlPct);
    decimal worst    = trades.Min(t => t.PnlPct);
    decimal avgBars  = (decimal)trades.Average(t => t.BarsHeld);

    Console.WriteLine($"Total trades:     {trades.Count}");
    Console.WriteLine($"Wins / Losses:    {wins} / {losses}  (win rate {(decimal)wins / trades.Count:P1})");
    Console.WriteLine($"Avg P&L/trade:    {avgPnl:P1}");
    Console.WriteLine($"Sum of P&L%:      {totalPnl:P1}  (naive sum, NOT compounded)");
    Console.WriteLine($"Best / Worst:     {best:P1} / {worst:P1}");
    Console.WriteLine($"Avg bars held:    {avgBars:F1}  (x15min = {avgBars * 15:F0} min)");
    Console.WriteLine();
    Console.WriteLine("Exit reason breakdown:");
    foreach (var g in trades.GroupBy(t => t.Reason))
        Console.WriteLine($"  {g.Key,-32} {g.Count(),3}  avg P&L {g.Average(t => t.PnlPct):P1}");
}
