using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

/// <summary>
/// Account-wide, cross-strategy daily risk circuit breaker shared by
/// ElonMoneyFunction and ReversalCallFunction. Backed by Azure Table Storage
/// (single row, "RiskState"/"Daily") so the day's counters survive host
/// restarts.
///
/// Tracks, for the current ET trading day:
///   - DayTrades         — number of option round-trips OPENED AND CLOSED
///                          on the same calendar day (a rough proxy for the
///                          PDT day-trade count; a small/margin account that
///                          racks up 4+ same-day round trips in 5 business
///                          days gets flagged Pattern Day Trader).
///   - ConsecutiveLosses — losing trades in a row (any strategy).
///   - RealizedPnl       — sum of (exitPremium - entryPremium) * 100 across
///                          all trades closed today.
///   - DailyStartCash    — tradable cash captured at the first entry attempt
///                          of the day (used as the denominator for the
///                          daily-loss-% breaker).
///
/// If any threshold is breached, Halted=true for the rest of the day and
/// BOTH functions skip new entries (exits / EOD force-closes still run).
/// </summary>
public sealed class RiskState
{
    private const string TableName    = "RiskState";
    private const string PartitionKey = "RiskState";
    private const string RowKey       = "Daily";

    // ── Tuneable thresholds ───────────────────────────────────────────────────
    public const int     MaxDayTrades         = 3;     // stay under PDT's 4-in-5-business-days threshold
    public const int     MaxConsecutiveLosses = 3;     // pause after 3 losers in a row
    public const decimal MaxDailyLossPct      = 0.20m; // halt if realized loss exceeds 20% of the day's starting cash

    private readonly TableClient _table;
    private readonly ILogger<RiskState> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private DateOnly _date;
    private int      _dayTrades;
    private int      _consecutiveLosses;
    private decimal  _dailyStartCash;
    private decimal  _realizedPnl;
    private bool     _halted;
    private string   _haltReason = "";
    private bool     _loaded;

    public RiskState(IConfiguration config, ILogger<RiskState> logger)
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
            _logger.LogError(ex, "RiskState: failed to create/verify table '{Table}'.", TableName);
        }
    }

    private async Task EnsureLoadedAsync(DateOnly today)
    {
        await _lock.WaitAsync();
        try
        {
            if (_loaded && _date == today) return;

            if (!_loaded)
            {
                try
                {
                    var entity = await _table.GetEntityIfExistsAsync<TableEntity>(PartitionKey, RowKey);
                    if (entity.HasValue)
                    {
                        var e = entity.Value!;
                        var storedDate = DateOnly.Parse(e.GetString("Date") ?? today.ToString("yyyy-MM-dd"));
                        if (storedDate == today)
                        {
                            _date              = storedDate;
                            _dayTrades         = (int)(e.GetInt32("DayTrades") ?? 0);
                            _consecutiveLosses = (int)(e.GetInt32("ConsecutiveLosses") ?? 0);
                            _dailyStartCash    = (decimal)(e.GetDouble("DailyStartCash") ?? 0.0);
                            _realizedPnl       = (decimal)(e.GetDouble("RealizedPnl") ?? 0.0);
                            _halted            = e.GetBoolean("Halted") ?? false;
                            _haltReason        = e.GetString("HaltReason") ?? "";
                            _loaded = true;
                            return;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "RiskState: failed to load daily risk state — starting fresh.");
                }
            }

            // New day (or no prior row, or load failed) — reset counters.
            _date              = today;
            _dayTrades         = 0;
            _consecutiveLosses = 0;
            _dailyStartCash    = 0m;
            _realizedPnl       = 0m;
            _halted            = false;
            _haltReason        = "";
            _loaded            = true;

            await PersistAsync();
            _logger.LogInformation("RiskState: new trading day {Date} — risk counters reset.", today);
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task PersistAsync()
    {
        var entity = new TableEntity(PartitionKey, RowKey)
        {
            { "Date",              _date.ToString("yyyy-MM-dd") },
            { "DayTrades",         _dayTrades },
            { "ConsecutiveLosses", _consecutiveLosses },
            { "DailyStartCash",    (double)_dailyStartCash },
            { "RealizedPnl",       (double)_realizedPnl },
            { "Halted",            _halted },
            { "HaltReason",        _haltReason },
        };

        try
        {
            await _table.UpsertEntityAsync(entity, TableUpdateMode.Replace);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RiskState: failed to persist daily risk state.");
        }
    }

    /// <summary>
    /// Captures the day's starting cash on first call of the day (used as the
    /// denominator for the daily-loss-% breaker). Cheap to call on every entry
    /// attempt — only writes when DailyStartCash is not yet set for today.
    /// </summary>
    public async Task EnsureDailyStartCashAsync(DateOnly today, decimal currentCash)
    {
        await EnsureLoadedAsync(today);
        if (_dailyStartCash > 0m) return;

        await _lock.WaitAsync();
        try
        {
            if (_dailyStartCash > 0m) return;
            _dailyStartCash = currentCash;
            await PersistAsync();
            _logger.LogInformation("RiskState: captured daily starting cash ${Cash:F2} for {Date}.", currentCash, today);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>True if new entries should be skipped for the rest of the day.</summary>
    public async Task<bool> IsHaltedAsync(DateOnly today)
    {
        await EnsureLoadedAsync(today);
        return _halted;
    }

    public async Task<string> GetHaltReasonAsync(DateOnly today)
    {
        await EnsureLoadedAsync(today);
        return _haltReason;
    }

    /// <summary>
    /// Records a closed trade's P&amp;L and updates the circuit breaker.
    /// </summary>
    /// <param name="today">Current ET date.</param>
    /// <param name="pnlDollars">(exitPremium - entryPremium) * 100, per contract.</param>
    /// <param name="openedToday">True if the position was opened on the same ET calendar day it's being closed.</param>
    /// <param name="strategyTag">Short label for logging, e.g. "ElonMoney" or "ReversalCall".</param>
    public async Task RecordTradeClosedAsync(DateOnly today, decimal pnlDollars, bool openedToday, string strategyTag)
    {
        await EnsureLoadedAsync(today);

        await _lock.WaitAsync();
        try
        {
            if (openedToday)
                _dayTrades++;

            if (pnlDollars < 0m)
                _consecutiveLosses++;
            else
                _consecutiveLosses = 0;

            _realizedPnl += pnlDollars;

            if (!_halted)
            {
                if (_dayTrades >= MaxDayTrades)
                {
                    _halted     = true;
                    _haltReason = $"day-trade count reached {_dayTrades} (limit {MaxDayTrades}) — PDT guard";
                }
                else if (_consecutiveLosses >= MaxConsecutiveLosses)
                {
                    _halted     = true;
                    _haltReason = $"{_consecutiveLosses} consecutive losing trades (limit {MaxConsecutiveLosses})";
                }
                else if (_dailyStartCash > 0m && _realizedPnl <= -_dailyStartCash * MaxDailyLossPct)
                {
                    _halted     = true;
                    _haltReason = $"realized loss ${-_realizedPnl:F2} reached {MaxDailyLossPct:P0} of starting cash ${_dailyStartCash:F2}";
                }
            }

            await PersistAsync();

            _logger.LogInformation(
                "RiskState [{Strategy}]: trade closed pnl=${Pnl:F2} openedToday={OpenedToday}  " +
                "→ dayTrades={DayTrades}/{MaxDayTrades}  consecLosses={ConsecLosses}/{MaxConsecLosses}  " +
                "realizedPnl=${Realized:F2} (start cash ${StartCash:F2})  halted={Halted}{Reason}.",
                strategyTag, pnlDollars, openedToday, _dayTrades, MaxDayTrades, _consecutiveLosses, MaxConsecutiveLosses,
                _realizedPnl, _dailyStartCash, _halted, _halted ? $" — {_haltReason}" : "");
        }
        finally
        {
            _lock.Release();
        }
    }
}
