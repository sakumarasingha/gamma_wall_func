using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

/// <summary>
/// Tracks open call positions opened by ReversalCallFunction.
/// State is held in-memory for the lifetime of the Functions host process.
/// Note: a host restart clears all state — ensure no positions are open before redeploying.
/// </summary>
public sealed class ReversalPositionStore
{
    public sealed class Position
    {
        public required string   OptionSymbol    { get; init; }
        public required decimal  EntryPremium    { get; init; }
        public required decimal  EntryUnderlying { get; init; }
        public required DateTime EntryTime       { get; init; }
        // Highest premium observed since entry — used for the trailing stop.
        public decimal PeakPremium    { get; set; }
        // True once profit has reached TrailingActivatePct.
        public bool    TrailingActive { get; set; }
    }

    private readonly ConcurrentDictionary<string, Position> _cache = new();
    private readonly ILogger<ReversalPositionStore> _logger;

    public ReversalPositionStore(ILogger<ReversalPositionStore> logger)
    {
        _logger = logger;
        _logger.LogInformation("ReversalPositionStore: initialized (in-memory).");
    }

    public Task<bool>                  HasOpenPositionAsync(string ticker) => Task.FromResult(_cache.ContainsKey(ticker));
    public Task<Position?>             TryGetAsync(string ticker)          => Task.FromResult(_cache.TryGetValue(ticker, out var p) ? p : null);
    public Task<IReadOnlyList<string>> GetOpenTickersAsync()               => Task.FromResult<IReadOnlyList<string>>(_cache.Keys.ToList());
    public Task<bool>                  IsEmptyAsync()                      => Task.FromResult(_cache.IsEmpty);

    public Task OpenAsync(
        string ticker, string optionSymbol, decimal entryPremium,
        decimal entryUnderlying, DateTime entryTime)
    {
        var pos = new Position
        {
            OptionSymbol    = optionSymbol,
            EntryPremium    = entryPremium,
            EntryUnderlying = entryUnderlying,
            EntryTime       = entryTime,
            PeakPremium     = entryPremium,
            TrailingActive  = false,
        };
        _cache[ticker] = pos;

        _logger.LogInformation(
            "ReversalPositionStore: OPENED — {Ticker}  symbol={Symbol}  " +
            "entryPremium=${Entry:F2}  underlying=${Price:F2}  openPositions={Count}.",
            ticker, optionSymbol, entryPremium, entryUnderlying, _cache.Count);

        return Task.CompletedTask;
    }

    // Mutations to Position properties are reflected in-memory immediately — no-op save needed.
    public Task SaveAsync(string ticker, Position pos) => Task.CompletedTask;

    public Task CloseAsync(string ticker)
    {
        if (_cache.TryRemove(ticker, out var pos))
        {
            _logger.LogInformation(
                "ReversalPositionStore: CLOSED — {Ticker}  symbol={Symbol}  " +
                "entryPremium=${Entry:F2}  openPositions={Count}.",
                ticker, pos.OptionSymbol, pos.EntryPremium, _cache.Count);
        }
        return Task.CompletedTask;
    }
}
