using System;
using System.Collections.Generic;
using System.Linq;
using Alpaca.Markets;

/// <summary>
/// Detects a "hammer / dragonfly-doji" bullish reversal pattern on the most
/// recently completed bar in a series, with standard confirmation checks:
///   • Downtrend leading into the candle
///   • Hammer/dragonfly-doji shape (long lower wick, little/no upper wick)
///   • RSI oversold reading at the candle
///   • Candle low sits at/near recent support (the local low)
///   • Candle volume above the recent average (participation confirmation)
///
/// No separate "confirmation candle" is required — the decision is made on
/// the latest completed bar alone so it can be checked the moment that bar
/// closes.
/// </summary>
public static class PatternDetector
{
    public const int     RsiPeriod             = 14;
    public const double  RsiOversoldThreshold  = 40.0;
    public const int     SupportLookbackBars   = 10;
    public const decimal SupportTolerancePct   = 0.015m; // candle low within 1.5% of recent low counts as "at support"
    public const decimal LowerWickMinRangePct  = 0.50m;  // lower wick must be >= 50% of the candle's range
    public const decimal UpperWickMaxRangePct  = 0.35m;  // upper wick must be <= 35% of the candle's range

    public record ReversalResult(
        bool    Detected,
        bool    Downtrend,
        bool    HammerShape,
        double  Rsi,
        bool    Oversold,
        bool    NearSupport,
        bool    VolumeConfirmed,
        decimal SupportLevel,
        decimal CandleLow,
        decimal CandleClose,
        long    CandleVolume,
        double  AvgVolume,
        string  Description);

    /// <summary>
    /// Evaluates bars[^1] (the most recently completed bar) as the candidate
    /// hammer/reversal candle. Requires at least RsiPeriod + SupportLookbackBars + 1
    /// bars total.
    /// </summary>
    public static ReversalResult DetectBullishReversal(IReadOnlyList<IBar> bars)
    {
        int minBarsNeeded = RsiPeriod + SupportLookbackBars + 1;
        if (bars.Count < minBarsNeeded)
        {
            return new ReversalResult(false, false, false, 0, false, false, false,
                0m, 0m, 0m, 0, 0, $"not enough bars ({bars.Count}/{minBarsNeeded})");
        }

        int n = bars.Count;
        var candle = bars[n - 1]; // most recently completed bar — the candidate

        decimal open  = (decimal)candle.Open;
        decimal close = (decimal)candle.Close;
        decimal high  = (decimal)candle.High;
        decimal low   = (decimal)candle.Low;
        decimal body      = Math.Abs(close - open);
        decimal range     = high - low;
        decimal lowerWick = Math.Min(open, close) - low;
        decimal upperWick = high - Math.Max(open, close);

        // ── 1. Downtrend leading into the candle ─────────────────────────────
        var trendWindow = bars.Skip(n - 1 - SupportLookbackBars).Take(SupportLookbackBars).ToList();
        bool downtrend = trendWindow.Count >= 2 && trendWindow[^1].Close < trendWindow[0].Close;

        // ── 2. Hammer / dragonfly-doji shape ─────────────────────────────────
        bool hammerShape = range > 0m
            && lowerWick >= range * LowerWickMinRangePct
            && upperWick <= range * UpperWickMaxRangePct
            && (body <= range * 0.10m || lowerWick >= body * 2m);

        // ── 3. RSI oversold reading as of this candle ────────────────────────
        var rsiCloses = bars.Skip(n - 1 - RsiPeriod).Take(RsiPeriod + 1).Select(b => (double)b.Close).ToList();
        double rsi = ComputeRsi(rsiCloses);
        bool oversold = rsi < RsiOversoldThreshold;

        // ── 4. Candle low sits at/near recent support ────────────────────────
        var supportWindow = bars.Skip(n - 1 - SupportLookbackBars).Take(SupportLookbackBars + 1)
                                 .Select(b => (decimal)b.Low).ToList();
        decimal supportLevel = supportWindow.Min();
        bool nearSupport = low <= supportLevel * (1m + SupportTolerancePct);

        // ── 5. Volume confirmation — candle volume above recent average ─────
        var volWindow = bars.Skip(n - 1 - SupportLookbackBars).Take(SupportLookbackBars)
                             .Select(b => (double)b.Volume).ToList();
        double avgVolume = volWindow.Count > 0 ? volWindow.Average() : 0;
        bool volumeConfirmed = (double)candle.Volume > avgVolume;

        bool detected = downtrend && hammerShape && oversold && nearSupport && volumeConfirmed;

        string description =
            $"downtrend={downtrend}, hammerShape={hammerShape}, " +
            $"RSI={rsi:F1}(oversold<{RsiOversoldThreshold}={oversold}), " +
            $"support={supportLevel:F2}(candleLow={low:F2}, near={nearSupport}), " +
            $"volume={candle.Volume:F0} vs avg{SupportLookbackBars}={avgVolume:F0}(confirmed={volumeConfirmed})";

        return new ReversalResult(detected, downtrend, hammerShape, rsi, oversold,
            nearSupport, volumeConfirmed, supportLevel, low, close, (long)candle.Volume, avgVolume, description);
    }

    // Simple-average RSI (Wilder's smoothing not used — fine for a 14-period rolling check).
    private static double ComputeRsi(List<double> closes)
    {
        if (closes.Count < 2) return 50.0;

        double gainSum = 0, lossSum = 0;
        for (int i = 1; i < closes.Count; i++)
        {
            double diff = closes[i] - closes[i - 1];
            if (diff > 0) gainSum += diff;
            else          lossSum -= diff;
        }

        int periods = closes.Count - 1;
        double avgGain = gainSum / periods;
        double avgLoss = lossSum / periods;

        if (avgLoss == 0) return avgGain == 0 ? 50.0 : 100.0;

        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }
}
