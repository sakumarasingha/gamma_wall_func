using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

internal static class TradingState
{
    // Open call positions (underlying ticker → option symbol)
    internal static readonly ConcurrentDictionary<string, string>  OpenOptions     = new();
    // Underlying price at time of entry (for P&L reporting)
    internal static readonly ConcurrentDictionary<string, decimal> EntryPrices     = new();
    // Option premium (ask) at time of entry
    internal static readonly ConcurrentDictionary<string, decimal> EntryPremiums   = new();
    // Entry timestamp (for reference / cooldown logic)
    internal static readonly ConcurrentDictionary<string, DateTime> EntryTimes     = new();

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
            logger.LogInformation("TradingState: new trading day {Date} — resetting daily state.", today);
            _lastTradingDate = today;
            _voldHistory.Clear();
            // Note: OpenOptions / EntryPrices / EntryPremiums / EntryTimes are
            // intentionally NOT cleared on day rollover — an open position
            // should keep being monitored for exit across the day boundary
            // (e.g. if it's still open near the close).
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

    internal static void ClearPosition(string ticker)
    {
        OpenOptions.TryRemove(ticker, out _);
        EntryPrices.TryRemove(ticker, out _);
        EntryPremiums.TryRemove(ticker, out _);
        EntryTimes.TryRemove(ticker, out _);
    }
}
