using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

/// <summary>
/// Tracks open call positions opened by ReversalCallFunction, including the
/// rolling "peak premium" needed for the trailing-stop exit logic — backed by
/// Azure Table Storage so the state survives Functions host restarts / cold
/// starts (a Consumption plan can scale to zero between invocations, which
/// would otherwise silently wipe an in-memory-only dictionary and leave an
/// open option position with no exit management).
///
/// Registered as a singleton; keeps an in-memory cache for fast repeated
/// reads within/across invocations of the same warm instance, and mirrors
/// every write to the "ReversalPositions" table so a cold instance can
/// reload the open positions on first use.
/// </summary>
public sealed class ReversalPositionStore
{
    private const string TableName    = "ReversalPositions";
    private const string PartitionKey = "ReversalPosition";

    public sealed class Position
    {
        public required string   OptionSymbol    { get; init; }
        public required decimal  EntryPremium    { get; init; }
        public required decimal  EntryUnderlying { get; init; }
        public required DateTime EntryTime       { get; init; }

        // Highest premium observed since entry — used for the trailing stop.
        public decimal PeakPremium    { get; set; }
        // True once profit has reached TrailingActivatePct (15%) — once true,
        // a pullback of TrailingStepPct (5%) from the peak closes the trade.
        public bool    TrailingActive { get; set; }
        // GEX wall level captured at entry (first significant positive-GEX
        // strike above the underlying price at the time of entry). When the
        // underlying approaches this level, we exit before the pinning zone
        // stalls momentum. 0 = no wall was identified at entry time.
        public decimal GexWallAbove   { get; set; }
    }

    private readonly TableClient _table;
    private readonly ILogger<ReversalPositionStore> _logger;
    private readonly ConcurrentDictionary<string, Position> _cache = new();
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private volatile bool _loaded;

    public ReversalPositionStore(IConfiguration config, ILogger<ReversalPositionStore> logger)
    {
        _logger = logger;

        string connectionString =
            config["AzureWebJobsStorage"]
            ?? Environment.GetEnvironmentVariable("AzureWebJobsStorage")
            ?? "UseDevelopmentStorage=true";

        _table = new TableClient(connectionString, TableName);

        try
        {
            _table.CreateIfNotExists();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalPositionStore: failed to create/verify table '{Table}'.", TableName);
        }
    }

    // ── Load (once per warm instance) ────────────────────────────────────────

    private async Task EnsureLoadedAsync()
    {
        if (_loaded) return;

        await _loadLock.WaitAsync();
        try
        {
            if (_loaded) return;

            await foreach (var entity in _table.QueryAsync<TableEntity>(e => e.PartitionKey == PartitionKey))
            {
                var pos = new Position
                {
                    OptionSymbol    = entity.GetString("OptionSymbol") ?? "",
                    EntryPremium    = (decimal)(entity.GetDouble("EntryPremium")    ?? 0.0),
                    EntryUnderlying = (decimal)(entity.GetDouble("EntryUnderlying") ?? 0.0),
                    EntryTime       = entity.GetDateTimeOffset("EntryTime")?.DateTime ?? DateTime.UtcNow,
                    PeakPremium     = (decimal)(entity.GetDouble("PeakPremium")     ?? 0.0),
                    TrailingActive  = entity.GetBoolean("TrailingActive") ?? false,
                    GexWallAbove    = (decimal)(entity.GetDouble("GexWallAbove")    ?? 0.0),
                };
                _cache[entity.RowKey] = pos;

                _logger.LogInformation(
                    "ReversalPositionStore: restored open position — {Ticker} {Symbol}  " +
                    "entryPremium=${Entry:F2}  peak=${Peak:F2}  trailingActive={Trailing}  gexWall=${Wall:F2}.",
                    entity.RowKey, pos.OptionSymbol, pos.EntryPremium, pos.PeakPremium, pos.TrailingActive, pos.GexWallAbove);
            }

            int count = _cache.Count;
            if (count > 0)
                _logger.LogInformation("ReversalPositionStore: loaded {Count} open position(s) from table storage.", count);

            _loaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalPositionStore: failed to load positions from table storage — starting with empty cache.");
            _loaded = true; // don't retry on every call; in-memory cache remains usable for this run
        }
        finally
        {
            _loadLock.Release();
        }
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    public async Task<bool> HasOpenPositionAsync(string ticker)
    {
        await EnsureLoadedAsync();
        return _cache.ContainsKey(ticker);
    }

    public async Task<Position?> TryGetAsync(string ticker)
    {
        await EnsureLoadedAsync();
        return _cache.TryGetValue(ticker, out var pos) ? pos : null;
    }

    public async Task<IReadOnlyList<string>> GetOpenTickersAsync()
    {
        await EnsureLoadedAsync();
        return _cache.Keys.ToList();
    }

    public async Task<bool> IsEmptyAsync()
    {
        await EnsureLoadedAsync();
        return _cache.IsEmpty;
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    public async Task OpenAsync(
        string ticker, string optionSymbol, decimal entryPremium,
        decimal entryUnderlying, DateTime entryTime, decimal gexWallAbove = 0m)
    {
        await EnsureLoadedAsync();

        var pos = new Position
        {
            OptionSymbol    = optionSymbol,
            EntryPremium    = entryPremium,
            EntryUnderlying = entryUnderlying,
            EntryTime       = entryTime,
            PeakPremium     = entryPremium,
            TrailingActive  = false,
            GexWallAbove    = gexWallAbove,
        };

        _cache[ticker] = pos;
        await UpsertAsync(ticker, pos);
    }

    /// Persists the current (possibly mutated) PeakPremium / TrailingActive
    /// fields for an already-open position back to table storage. Call this
    /// after updating pos.PeakPremium / pos.TrailingActive in the exit check.
    public Task SaveAsync(string ticker, Position pos) => UpsertAsync(ticker, pos);

    public async Task CloseAsync(string ticker)
    {
        await EnsureLoadedAsync();
        _cache.TryRemove(ticker, out _);

        try
        {
            await _table.DeleteEntityAsync(PartitionKey, ticker);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            // already gone — fine
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalPositionStore: failed to delete row for {Ticker}.", ticker);
        }
    }

    private async Task UpsertAsync(string ticker, Position pos)
    {
        var entity = new TableEntity(PartitionKey, ticker)
        {
            { "OptionSymbol",    pos.OptionSymbol },
            { "EntryPremium",    (double)pos.EntryPremium },
            { "EntryUnderlying", (double)pos.EntryUnderlying },
            { "EntryTime",       new DateTimeOffset(DateTime.SpecifyKind(pos.EntryTime, DateTimeKind.Unspecified), TimeSpan.Zero) },
            { "PeakPremium",     (double)pos.PeakPremium },
            { "TrailingActive",  pos.TrailingActive },
            { "GexWallAbove",    (double)pos.GexWallAbove },
        };

        try
        {
            await _table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReversalPositionStore: failed to upsert row for {Ticker}.", ticker);
        }
    }
}
