using System;
using System.Collections.Generic;
using Alpaca.Markets;

public static class VoldCalculator
{
    public record VoldResult(
        double VoldRatio,
        long   UpVolume,
        long   DownVolume,
        long   NeutralVolume,
        long   TotalVolume,
        double UpPct,
        double DownPct,
        int    CandleCount,
        string Signal
    );

    public static VoldResult Calculate(IEnumerable<IBar> bars)
    {
        long upVolume      = 0;
        long downVolume    = 0;
        long neutralVolume = 0;
        long totalVolume   = 0;
        int  candleCount   = 0;

        foreach (var bar in bars)
        {
            long v = (long)bar.Volume;
            totalVolume += v;
            candleCount++;

            if (bar.Close > bar.Open)       upVolume      += v;
            else if (bar.Close < bar.Open)  downVolume    += v;
            else                            neutralVolume += v;
        }

        double voldRatio = downVolume == 0
            ? (upVolume > 0 ? double.PositiveInfinity : 0.0)
            : Math.Round((double)upVolume / downVolume, 4);

        double upPct   = totalVolume > 0 ? Math.Round((double)upVolume   / totalVolume * 100, 1) : 0;
        double downPct = totalVolume > 0 ? Math.Round((double)downVolume / totalVolume * 100, 1) : 0;

        return new VoldResult(
            VoldRatio:     voldRatio,
            UpVolume:      upVolume,
            DownVolume:    downVolume,
            NeutralVolume: neutralVolume,
            TotalVolume:   totalVolume,
            UpPct:         upPct,
            DownPct:       downPct,
            CandleCount:   candleCount,
            Signal:        Signal(voldRatio)
        );
    }

    private static string Signal(double ratio) => ratio switch
    {
        double.PositiveInfinity => "STRONG BULL",
        >= 4.0 => "STRONG BULL",
        >= 2.0 => "BULL",
        >= 1.0 => "MILD BULL",
        >= 0.5 => "MILD BEAR",
        >  0   => "BEAR",
        _      => "NO DATA",
    };
}
