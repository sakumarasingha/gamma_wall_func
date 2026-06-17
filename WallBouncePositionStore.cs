using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

/// <summary>
/// Tracks open call positions opened by WallBounceFunction.
/// State is held in-memory for the lifetime of the Functions host process.
/// Note: a host restart clears all state — ensure no positions are open before redeploying.
/// </summary>
public sealed class WallBouncePositionStore
{
    public sealed class Position
    {
        public required string   OptionSymbol    { get; init; }
        public required decimal  EntryPremium    { get; init; }
        public required decimal  EntryUnderlying { get; init; }
        public required DateTime EntryTime       { get; init; }
        // Put wall captured at entry — exit immediately if price closes below this.
        public required decimal  PutWallLevel    { get; init; }
        // High-water mark of the trailing stop level (currentPremium × 0.98, ratchets up only).
        public decimal           PeakPremium     { get; set; }
        // True once premium has gained ≥ 5% from entry.
        public bool              TrailingActive  { get; set; }
    }

    private readonly ConcurrentDictionary<string, Position> _cache = new();
    private readonly ILogger<WallBouncePositionStore> _logger;

    public WallBouncePositionStore(ILogger<WallBouncePositionStore> logger)
    {
        _logger = logger;
        _logger.LogInformation("WallBouncePositionStore: initialized (in-memory).");
    }

    public Task<bool>                  HasOpenPositionAsync(string ticker) => Task.FromResult(_cache.ContainsKey(ticker));
    public Task<Position?>             TryGetAsync(string ticker)          => Task.FromResult(_cache.TryGetValue(ticker, out var p) ? p : null);
    public Task<IReadOnlyList<string>> GetOpenTickersAsync()               => Task.FromResult<IReadOnlyList<string>>(_cache.Keys.ToList());
    public Task<bool>                  IsEmptyAsync()                      => Task.FromResult(_cache.IsEmpty);

    public Task OpenAsync(
        string ticker, string optionSymbol, decimal entryPremium,
        decimal entryUnderlying, DateTime entryTime, decimal putWallLevel)
    {
        _cache[ticker] = new Position
        {
            OptionSymbol    = optionSymbol,
            EntryPremium    = entryPremium,
            EntryUnderlying = entryUnderlying,
            EntryTime       = entryTime,
            PutWallLevel    = putWallLevel,
            PeakPremium     = entryPremium,
            TrailingActive  = false,
        };

        _logger.LogInformation(
            "WallBouncePositionStore: OPENED — {Ticker}  symbol={Symbol}  " +
            "entryPremium=${Entry:F2}  underlying=${Price:F2}  putWall=${Wall:F2}  openPositions={Count}.",
            ticker, optionSymbol, entryPremium, entryUnderlying, putWallLevel, _cache.Count);

        return Task.CompletedTask;
    }

    // Mutations to Position properties are reflected in-memory immediately — no-op save needed.
    public Task SaveAsync(string ticker, Position pos) => Task.CompletedTask;

    public Task CloseAsync(string ticker)
    {
        if (_cache.TryRemove(ticker, out var pos))
        {
            _logger.LogInformation(
                "WallBouncePositionStore: CLOSED — {Ticker}  symbol={Symbol}  " +
                "entryPremium=${Entry:F2}  putWall=${Wall:F2}  openPositions={Count}.",
                ticker, pos.OptionSymbol, pos.EntryPremium, pos.PutWallLevel, _cache.Count);
        }
        return Task.CompletedTask;
    }
}
