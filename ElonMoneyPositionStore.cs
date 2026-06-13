using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

/// <summary>
/// Tracks ElonMoneyFunction's open call position (one per ticker), backed by
/// Azure Table Storage so it survives Functions host restarts / cold starts —
/// same rationale as ReversalPositionStore.
/// </summary>
public sealed class ElonMoneyPositionStore
{
    private const string TableName    = "ElonMoneyPositions";
    private const string PartitionKey = "ElonMoneyPosition";

    public sealed class Position
    {
        public required string   OptionSymbol    { get; init; }
        public required decimal  EntryPremium    { get; init; }
        public required decimal  EntryUnderlying { get; init; }
        public required DateTime EntryTime       { get; init; }
    }

    private readonly TableClient _table;
    private readonly ILogger<ElonMoneyPositionStore> _logger;
    private readonly ConcurrentDictionary<string, Position> _cache = new();
    private readonly SemaphoreSlim _loadLock = new(1, 1);
    private volatile bool _loaded;

    public ElonMoneyPositionStore(IConfiguration config, ILogger<ElonMoneyPositionStore> logger)
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
            _logger.LogError(ex, "ElonMoneyPositionStore: failed to create/verify table '{Table}'.", TableName);
        }
    }

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
                };
                _cache[entity.RowKey] = pos;

                _logger.LogInformation(
                    "ElonMoneyPositionStore: restored open position — {Ticker} {Symbol}  entryPremium=${Entry:F2}.",
                    entity.RowKey, pos.OptionSymbol, pos.EntryPremium);
            }

            if (_cache.Count > 0)
                _logger.LogInformation("ElonMoneyPositionStore: loaded {Count} open position(s) from table storage.", _cache.Count);

            _loaded = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ElonMoneyPositionStore: failed to load positions from table storage — starting with empty cache.");
            _loaded = true;
        }
        finally
        {
            _loadLock.Release();
        }
    }

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

    public async Task OpenAsync(string ticker, string optionSymbol, decimal entryPremium, decimal entryUnderlying, DateTime entryTime)
    {
        await EnsureLoadedAsync();

        var pos = new Position
        {
            OptionSymbol    = optionSymbol,
            EntryPremium    = entryPremium,
            EntryUnderlying = entryUnderlying,
            EntryTime       = entryTime,
        };

        _cache[ticker] = pos;

        var entity = new TableEntity(PartitionKey, ticker)
        {
            { "OptionSymbol",    pos.OptionSymbol },
            { "EntryPremium",    (double)pos.EntryPremium },
            { "EntryUnderlying", (double)pos.EntryUnderlying },
            { "EntryTime",       new DateTimeOffset(DateTime.SpecifyKind(pos.EntryTime, DateTimeKind.Unspecified), TimeSpan.Zero) },
        };

        try
        {
            await _table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ElonMoneyPositionStore: failed to upsert row for {Ticker}.", ticker);
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
            _logger.LogError(ex, "ElonMoneyPositionStore: failed to delete row for {Ticker}.", ticker);
        }
    }
}
