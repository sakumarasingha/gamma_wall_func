public static class Tickers
{
    // GexWatchlist — subset of liquid names with high options volume; GEX
    // signals are most reliable when dealer hedging flows dominate the chain
    // (i.e. the biggest, most-traded names).  Kept deliberately narrow —
    // better to trade fewer names well than to fire GEX signals on thinly-
    // traded chains where the proxy is unreliable.
    public static readonly string[] GexWatchlist =
    [
        "TSLA",
        "AAPL",
        "NVDA",
        "MSFT",
        "SPY",
        "QQQ",
    ];
}
