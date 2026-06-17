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
/// Tracks open call positions managed by GexFunction, backed by Azure Table
/// Storage so the state survives Functions host restarts / cold starts.
///
/// Each row represents one open position:
///   PartitionKey = "GexPosition"
///   RowKey       = ticker (e.g. "TSLA")
///
/// Stored fields per position:
///   OptionSymbol    — OCC symbol of the bought call
///   EntryPremium    — ask price paid at entry
///   EntryUnderlying — underlying price at entry
///   EntryTime       — UTC datetime of entry
///   GexWallAbove    — nearest significant +GEX strike above entry price
///                     (0 = none found); used for the wall-exit signal
/// </summary>
public sealed class GexPositionStore
{
    private const string TableName    = "GexPositions";
    private const string PartitionKey = "GexPosition";

    public sealed class Position
    {
        public required string   OptionSymbol    { get; init; }
        public required decimal  EntryPremium    { get; init; }
        public required decimal  EntryUnderlying { get; init; }
        public required DateTime EntryTime       { get; init; }
        // GEX wall captured at entry — close when underlying approaches this level
        public decimal           GexWallAbove    { get; init; }
    }

    private readonly TableClient _table;
    private readonly ILogger<GexPositionStore> _logger;
    private readonly ConcurrentDictionary<string, Position> _cache = new();
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private volatile bool _loaded;

    public GexPositionStore(IConfiguration config, ILogger<GexPositionStore> logger)
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
            _logger.LogError(ex, "GexPositionStore: failed to create/verify table '{Table}'.", TableName);
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
                    GexWallAbove    = (decimal)(entity.GetDouble("GexWallAbove")    ?? 0.0),
                };
                _cache[entity.RowKey] = pos;

                _logger.LogInformation(
                    "GexPositionStore: restored — {Ticker} {Symbol}  entryPremium=${Entry:F2}  gexWall=${Wall:F2}.",
                    entity.RowKey, pos.OptionSymbol, pos.EntryPremium, pos.GexWallAbove);
            }

            if (_cache.Count > 0)
                _logger.LogInformation("GexPositionStore: loaded {Count} open position(s) from table storage.", _cache.Count);

            _loaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexPositionStore: failed to load from table storage — starting with empty cache.");
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
        decimal entryUnderlying, DateTime entryTime, decimal gexWallAbove)
    {
        await EnsureLoadedAsync();

        var pos = new Position
        {
            OptionSymbol    = optionSymbol,
            EntryPremium    = entryPremium,
            EntryUnderlying = entryUnderlying,
            EntryTime       = entryTime,
            GexWallAbove    = gexWallAbove,
        };

        _cache[ticker] = pos;

        var entity = new TableEntity(PartitionKey, ticker)
        {
            { "OptionSymbol",    pos.OptionSymbol },
            { "EntryPremium",    (double)pos.EntryPremium },
            { "EntryUnderlying", (double)pos.EntryUnderlying },
            { "EntryTime",       new DateTimeOffset(DateTime.SpecifyKind(pos.EntryTime, DateTimeKind.Unspecified), TimeSpan.Zero) },
            { "GexWallAbove",    (double)pos.GexWallAbove },
        };

        try
        {
            await _table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GexPositionStore: failed to upsert row for {Ticker}.", ticker);
        }
    }

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
            _logger.LogError(ex, "GexPositionStore: failed to delete row for {Ticker}.", ticker);
        }
    }
}
