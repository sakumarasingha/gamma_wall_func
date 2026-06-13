public static class Tickers
{
    // Watchlist — currently just TSLA, but kept as an array so more tickers
    // can be added later without code changes elsewhere.
    public static readonly string[] Watchlist =
    [
        "TSLA",
    ];

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
}
