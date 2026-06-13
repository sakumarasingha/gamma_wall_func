using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

/// <summary>
/// In-memory VOLD-history tracker for ElonMoneyFunction. Open-position state
/// now lives in ElonMoneyPositionStore (Azure Table Storage) — see that class
/// for OptionSymbol / EntryPremium / EntryUnderlying / EntryTime.
///
/// VOLD history is intentionally kept in-memory only: it resets every trading
/// day anyway, and if a cold start loses a few minutes of history, the
/// "3 rising/falling in a row" checks simply take a few extra ticks to
/// re-establish — low impact, not worth persisting.
/// </summary>
internal static class TradingState
{
    // Rolling VOLD history per ticker — used to detect 3 consecutive rises (entry)
    // or 3 consecutive falls (exit). Oldest → newest, capped at 4 readings.
    private static readonly ConcurrentDictionary<string, List<double>> _voldHistory = new();

    private static DateOnly       _lastTradingDate = DateOnly.MinValue;
    private static readonly object _dateLock       = new();

    internal static void ResetIfNewDay(DateOnly today, ILogger logger)
    {
        lock (_dateLock)
        {
            if (_lastTradingDate == today) return;
            logger.LogInformation("TradingState: new trading day {Date} — resetting VOLD history.", today);
            _lastTradingDate = today;
            _voldHistory.Clear();
        }
    }

    // Records a VOLD reading from each scan cycle. Keeps last 4 (enough to
    // detect 3 consecutive increases or decreases).
    internal static void RecordVold(string ticker, double voldRatio)
    {
        var history = _voldHistory.GetOrAdd(ticker, _ => new List<double>());
        lock (history)
        {
            history.Add(voldRatio);
            if (history.Count > 4) history.RemoveAt(0);
        }
    }

    internal static List<double> GetVoldHistory(string ticker)
    {
        var history = _voldHistory.GetOrAdd(ticker, _ => new List<double>());
        lock (history)
        {
            return new List<double>(history);
        }
    }

    // Returns true if the last 3 readings are strictly increasing
    // (history[^3] < history[^2] < history[^1]).
    internal static bool IsVoldRisingThreeInARow(string ticker)
    {
        var history = GetVoldHistory(ticker);
        if (history.Count < 3) return false;
        int n = history.Count;
        return history[n - 3] < history[n - 2] && history[n - 2] < history[n - 1];
    }

    // Returns true if the last 3 readings are strictly decreasing
    // (history[^3] > history[^2] > history[^1]).
    internal static bool IsVoldFallingThreeInARow(string ticker)
    {
        var history = GetVoldHistory(ticker);
        if (history.Count < 3) return false;
        int n = history.Count;
        return history[n - 3] > history[n - 2] && history[n - 2] > history[n - 1];
    }
}
