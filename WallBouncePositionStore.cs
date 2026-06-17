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
/// Tracks open call positions opened by WallBounceFunction, backed by Azure
/// Table Storage so state survives Functions host restarts / cold starts.
///
/// Each row represents one open position:
///   PartitionKey = "WallBouncePosition"
///   RowKey       = ticker (e.g. "TSLA")
///
/// Stored fields:
///   OptionSymbol    — OCC symbol of the bought call
///   EntryPremium    — ask price paid at entry
///   EntryUnderlying — underlying price at entry
///   EntryTime       — UTC datetime of entry
///   PutWallLevel    — the put-wall strike captured at entry; if price closes
///                     below this level the position is exited immediately
///                     (wall broken → dealer hedging flips from support to selling)
///   PeakPremium     — highest premium seen since entry; updated every 1-min tick
///   TrailingActive  — true once premium has gained ≥ 5% from entry
/// </summary>
public sealed class WallBouncePositionStore
{
    private const string TableName    = "WallBouncePositions";
    private const string PartitionKey = "WallBouncePosition";

    public sealed class Position
    {
        public required string   OptionSymbol    { get; init; }
        public required decimal  EntryPremium    { get; init; }
        public required decimal  EntryUnderlying { get; init; }
        public required DateTime EntryTime       { get; init; }
        public required decimal  PutWallLevel    { get; init; }  // stop reference: exit if price < this
        public decimal           PeakPremium     { get; set; }
        public bool              TrailingActive  { get; set; }
    }

    private readonly TableClient _table;
    private readonly ILogger<WallBouncePositionStore> _logger;
    private readonly ConcurrentDictionary<string, Position> _cache = new();
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private volatile bool _loaded;

    public WallBouncePositionStore(IConfiguration config, ILogger<WallBouncePositionStore> logger)
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
            _logger.LogError(ex, "WallBouncePositionStore: failed to create/verify table '{Table}'.", TableName);
        }
    }

    // ── Lazy load from Table Storage (once per warm instance) ────────────────

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
                    PutWallLevel    = (decimal)(entity.GetDouble("PutWallLevel")    ?? 0.0),
                    PeakPremium     = (decimal)(entity.GetDouble("PeakPremium")     ?? 0.0),
                    TrailingActive  = entity.GetBoolean("TrailingActive") ?? false,
                };
                _cache[entity.RowKey] = pos;

                _logger.LogInformation(
                    "WallBouncePositionStore: restored — {Ticker} {Symbol}  entry=${Entry:F2}  " +
                    "putWall=${Wall:F2}  peak=${Peak:F2}  trailing={Trailing}.",
                    entity.RowKey, pos.OptionSymbol, pos.EntryPremium,
                    pos.PutWallLevel, pos.PeakPremium, pos.TrailingActive);
            }

            if (_cache.Count > 0)
                _logger.LogInformation(
                    "WallBouncePositionStore: loaded {Count} open position(s) from table storage.", _cache.Count);

            _loaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WallBouncePositionStore: failed to load from table storage — starting with empty cache.");
            _loaded = true;
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
        decimal entryUnderlying, DateTime entryTime, decimal putWallLevel)
    {
        await EnsureLoadedAsync();

        var pos = new Position
        {
            OptionSymbol    = optionSymbol,
            EntryPremium    = entryPremium,
            EntryUnderlying = entryUnderlying,
            EntryTime       = entryTime,
            PutWallLevel    = putWallLevel,
            PeakPremium     = entryPremium,
            TrailingActive  = false,
        };

        _cache[ticker] = pos;
        await UpsertAsync(ticker, pos);
    }

    /// Persists updated PeakPremium / TrailingActive back to table storage.
    /// Call after mutating these fields during the exit check.
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
            _logger.LogError(ex, "WallBouncePositionStore: failed to delete row for {Ticker}.", ticker);
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
            { "PutWallLevel",    (double)pos.PutWallLevel },
            { "PeakPremium",     (double)pos.PeakPremium },
            { "TrailingActive",  pos.TrailingActive },
        };

        try
        {
            await _table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "WallBouncePositionStore: failed to upsert row for {Ticker}.", ticker);
        }
    }
}
