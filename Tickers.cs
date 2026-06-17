public static class Tickers
{
    // ReversalWatchlist — TSLA plus 10 other super-mega-cap, high-liquidity
    // names with deep, tight-spread options chains (used by ReversalCallFunction).
    public static readonly string[] ReversalWatchlist =
    [
        "TSLA",
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "GOOGL",
        "META",
        "AVGO",
        "NFLX",
        "AMD",
        "JPM",
    ];

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
