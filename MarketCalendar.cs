using System;
using System.Collections.Generic;

/// <summary>
/// Simple static list of NYSE/Nasdaq full-day market holidays (the exchanges
/// that list TSLA, AAPL, etc. all observe the same holiday calendar). Used as
/// a cheap pre-check so the timer functions skip Alpaca API calls entirely on
/// days the market is closed, even though the date falls on a weekday.
///
/// NOTE: this is a static list that needs to be extended for future years —
/// it currently covers 2025-2027. It does NOT account for early-close days
/// (e.g. the day after Thanksgiving, Christmas Eve) — those are regular
/// trading days here, just shorter, which is fine since both functions
/// already stop at 15:55 ET.
/// </summary>
internal static class MarketCalendar
{
    private static readonly HashSet<DateOnly> Holidays = new()
    {
        // ── 2025 ─────────────────────────────────────────────────────────────
        new DateOnly(2025, 1, 1),   // New Year's Day
        new DateOnly(2025, 1, 20),  // Martin Luther King Jr. Day
        new DateOnly(2025, 2, 17),  // Washington's Birthday (Presidents' Day)
        new DateOnly(2025, 4, 18),  // Good Friday
        new DateOnly(2025, 5, 26),  // Memorial Day
        new DateOnly(2025, 6, 19),  // Juneteenth
        new DateOnly(2025, 7, 4),   // Independence Day
        new DateOnly(2025, 9, 1),   // Labor Day
        new DateOnly(2025, 11, 27), // Thanksgiving Day
        new DateOnly(2025, 12, 25), // Christmas Day

        // ── 2026 ─────────────────────────────────────────────────────────────
        new DateOnly(2026, 1, 1),   // New Year's Day
        new DateOnly(2026, 1, 19),  // Martin Luther King Jr. Day
        new DateOnly(2026, 2, 16),  // Washington's Birthday (Presidents' Day)
        new DateOnly(2026, 4, 3),   // Good Friday
        new DateOnly(2026, 5, 25),  // Memorial Day
        new DateOnly(2026, 6, 19),  // Juneteenth
        new DateOnly(2026, 7, 3),   // Independence Day (observed — Jul 4 is a Saturday)
        new DateOnly(2026, 9, 7),   // Labor Day
        new DateOnly(2026, 11, 26), // Thanksgiving Day
        new DateOnly(2026, 12, 25), // Christmas Day

        // ── 2027 ─────────────────────────────────────────────────────────────
        new DateOnly(2027, 1, 1),   // New Year's Day
        new DateOnly(2027, 1, 18),  // Martin Luther King Jr. Day
        new DateOnly(2027, 2, 15),  // Washington's Birthday (Presidents' Day)
        new DateOnly(2027, 3, 26),  // Good Friday
        new DateOnly(2027, 5, 31),  // Memorial Day
        new DateOnly(2027, 6, 18),  // Juneteenth (observed — Jun 19 is a Saturday)
        new DateOnly(2027, 7, 5),   // Independence Day (observed — Jul 4 is a Sunday)
        new DateOnly(2027, 9, 6),   // Labor Day
        new DateOnly(2027, 11, 25), // Thanksgiving Day
        new DateOnly(2027, 12, 24), // Christmas Day (observed — Dec 25 is a Saturday)
    };

    /// <summary>True if the given date (ET) is a full-day NYSE/Nasdaq market holiday.</summary>
    internal static bool IsHoliday(DateTime dateEt) => Holidays.Contains(DateOnly.FromDateTime(dateEt));
}
