using System;
using System.Collections.Generic;
using System.Linq;
using Alpaca.Markets;

/// <summary>
/// Computes a Net Gamma Exposure (GEX) profile for a ticker from its option
/// chain snapshot.
///
/// True GEX formula:
///   GEX per strike = (Call Gamma × Call OI − Put Gamma × Put OI) × 100 × S
///
/// Alpaca's real-time option snapshots do not expose open interest, so this
/// implementation uses a proxy:
///   Net Gamma Proxy per strike = Σ(Call Gamma) − Σ(Put Gamma)
///
/// This is directionally accurate for liquid names (TSLA, AAPL, NVDA, …)
/// where high-OI strikes dominate the chain anyway.
///
/// Interpretation:
///   Positive net gamma → dealers are long gamma → they absorb order flow →
///     price tends to pin / mean-revert at that strike (gamma wall).
///   Negative net gamma → dealers are short gamma → they amplify order flow →
///     directional moves have more fuel; good environment for long options.
///
/// Usage:
///   var gex = GexAnalyzer.Analyse(chainPage.Items, underlyingPrice);
///   if (!gex.IsNegativeGammaZone) { /* skip entry — pinning likely */ }
///   if (gex.WallDistance < 0.02m)  { /* skip — no room to run    */ }
/// </summary>
internal static class GexAnalyzer
{
    // A strike must have net gamma ≥ this fraction of the chain-wide peak
    // to qualify as a meaningful "wall" (filters out small background noise).
    private const double WallThresholdFraction = 0.25;

    /// <summary>
    /// The GEX snapshot captured at entry.
    /// </summary>
    /// <param name="IsNegativeGammaZone">
    ///   True when the net gamma at (or nearest below) the current underlying
    ///   price is ≤ 0 — dealers are short gamma, so moves are amplified.
    /// </param>
    /// <param name="NearestWallAbove">
    ///   First significant positive-GEX strike above the underlying price
    ///   (≥ <see cref="WallThresholdFraction"/> × chain-peak).
    ///   0 when none is found.
    /// </param>
    /// <param name="WallDistance">
    ///   (NearestWallAbove / underlyingPrice) − 1.  0 when no wall is found.
    /// </param>
    /// <param name="FlipLevel">
    ///   First strike above the underlying price where net gamma flips from
    ///   negative to positive (the "GEX zero line").  0 when not applicable.
    /// </param>
    /// <param name="NearestPutWallBelow">
    ///   Nearest significant negative-GEX strike at or below the underlying price
    ///   (≥ <see cref="WallThresholdFraction"/> × chain-wide peak negative gamma).
    ///   Dealers who sold puts must buy the underlying as price falls toward this
    ///   level → acts as mechanical support / bounce zone.
    ///   0 when none is found.
    /// </param>
    /// <param name="PutWallProximity">
    ///   (underlyingPrice − NearestPutWallBelow) / underlyingPrice.
    ///   0.005 = price is 0.5% above the put wall (very close).
    ///   1.0 when no put wall is found.
    /// </param>
    public record GexProfile(
        bool    IsNegativeGammaZone,
        decimal NearestWallAbove,
        decimal WallDistance,
        decimal FlipLevel,
        decimal NearestPutWallBelow,
        decimal PutWallProximity);

    /// <summary>
    /// Analyses the option chain and returns a <see cref="GexProfile"/>
    /// relative to the current underlying price.
    /// </summary>
    public static GexProfile Analyse(
        IReadOnlyDictionary<string, IOptionSnapshot> chain,
        decimal underlyingPrice)
    {
        if (chain.Count == 0 || underlyingPrice <= 0m)
            return new GexProfile(false, 0m, 0m, 0m, 0m, 1m);

        // ── 1. Accumulate net gamma per strike ───────────────────────────────
        var netGamma = new Dictionary<decimal, double>();

        foreach (var (symbol, snapshot) in chain)
        {
            double gamma = (double)(snapshot.Greeks?.Gamma ?? 0m);
            if (gamma <= 0.0) continue;                  // skip zero/missing greeks

            decimal strike = ParseStrike(symbol);
            if (strike <= 0m) continue;

            bool call = IsCall(symbol);

            netGamma.TryAdd(strike, 0.0);
            netGamma[strike] += call ? gamma : -gamma;
        }

        if (netGamma.Count == 0)
            return new GexProfile(false, 0m, 0m, 0m, 0m, 1m);

        var sortedStrikes = netGamma.Keys.OrderBy(s => s).ToList();

        // ── 2. Determine the zone the underlying price currently sits in ─────
        //      Use the net gamma of the nearest strike at or below the price.
        decimal nearestBelow = sortedStrikes.LastOrDefault(s => s <= underlyingPrice);
        double  zoneGamma    = nearestBelow > 0m
            ? netGamma[nearestBelow]
            : netGamma[sortedStrikes[0]];           // price is below all strikes

        bool isNegativeZone = zoneGamma <= 0.0;

        // ── 3. Find nearest meaningful positive-GEX wall above current price ─
        double peakPositive  = netGamma.Values.Where(g => g > 0).DefaultIfEmpty(0).Max();
        double wallThreshold = peakPositive * WallThresholdFraction;

        decimal flipLevel   = 0m;
        decimal nearestWall = 0m;

        foreach (var strike in sortedStrikes.Where(s => s > underlyingPrice))
        {
            double g = netGamma[strike];

            // First strike above price where gamma flips positive = the "GEX zero line"
            if (flipLevel == 0m && g > 0.0)
                flipLevel = strike;

            // First strike above price that clears the wall threshold
            if (nearestWall == 0m && wallThreshold > 0.0 && g >= wallThreshold)
            {
                nearestWall = strike;
                break;
            }
        }

        decimal wallDistance = nearestWall > 0m
            ? nearestWall / underlyingPrice - 1m
            : 0m;

        // ── 4. Find nearest significant put wall AT or BELOW current price ────
        //      Put walls = strikes with strongly negative net gamma (put gamma
        //      dominates). Dealers who sold those puts must buy the underlying
        //      as price falls to that level → mechanical support / bounce zone.
        double peakNegative   = netGamma.Values.Where(g => g < 0).DefaultIfEmpty(0).Min(); // most negative value
        double putWallThresh  = peakNegative * WallThresholdFraction;                      // e.g. 25% of peak

        decimal nearestPutWall = 0m;

        // Walk strikes at/below current price from nearest to farthest
        foreach (var strike in sortedStrikes.Where(s => s <= underlyingPrice).OrderByDescending(s => s))
        {
            double g = netGamma[strike];
            // sufficiently negative (both values negative, so g <= threshold)
            if (putWallThresh < 0.0 && g <= putWallThresh)
            {
                nearestPutWall = strike;
                break;
            }
        }

        decimal putWallProximity = nearestPutWall > 0m
            ? (underlyingPrice - nearestPutWall) / underlyingPrice   // 0.005 = 0.5% above wall
            : 1m;                                                      // no wall → 100% away

        return new GexProfile(isNegativeZone, nearestWall, wallDistance, flipLevel,
                              nearestPutWall, putWallProximity);
    }

    // ── OCC symbol helpers ────────────────────────────────────────────────────
    //
    // OCC format: <Ticker><YYMMDD><C|P><8-digit-strike-×1000>
    // e.g. "TSLA250117C00250000"
    //       ^^^^  <- variable-length ticker prefix (letters only)
    //           ^^^^^^ <- date (YYMMDD)
    //                 ^ <- C or P
    //                  ^^^^^^^^ <- strike × 1000 (e.g. 00250000 → $250.00)

    internal static bool IsCall(string occSymbol)
    {
        int i = FirstDigitIndex(occSymbol);
        int cpIdx = i + 6;
        return cpIdx < occSymbol.Length && occSymbol[cpIdx] == 'C';
    }

    internal static decimal ParseStrike(string occSymbol)
    {
        try
        {
            int i     = FirstDigitIndex(occSymbol);
            int start = i + 7;   // skip 6 date digits + 1 C/P char
            if (start + 8 <= occSymbol.Length
             && long.TryParse(occSymbol.AsSpan(start, 8), out long raw))
                return raw / 1000m;
        }
        catch { }
        return 0m;
    }

    private static int FirstDigitIndex(string s)
    {
        int i = 0;
        while (i < s.Length && !char.IsDigit(s[i])) i++;
        return i;
    }
}
