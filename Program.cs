using System;
using Alpaca.Markets;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        // TRADING_MODE = "paper" uses ALPACA_PAPER_API_KEY / ALPACA_PAPER_SECRET_KEY
        // TRADING_MODE = "live"  uses ALPACA_API_KEY / ALPACA_SECRET_KEY  (default)
        bool isPaper = string.Equals(
            Environment.GetEnvironmentVariable("TRADING_MODE"), "paper",
            StringComparison.OrdinalIgnoreCase);

        string apiKey = isPaper
            ? (Environment.GetEnvironmentVariable("ALPACA_PAPER_API_KEY")    ?? throw new InvalidOperationException("ALPACA_PAPER_API_KEY not set"))
            : (Environment.GetEnvironmentVariable("ALPACA_API_KEY")          ?? throw new InvalidOperationException("ALPACA_API_KEY not set"));

        string apiSecret = isPaper
            ? (Environment.GetEnvironmentVariable("ALPACA_PAPER_SECRET_KEY") ?? throw new InvalidOperationException("ALPACA_PAPER_SECRET_KEY not set"))
            : (Environment.GetEnvironmentVariable("ALPACA_SECRET_KEY")       ?? throw new InvalidOperationException("ALPACA_SECRET_KEY not set"));

        var env        = isPaper ? Alpaca.Markets.Environments.Paper : Alpaca.Markets.Environments.Live;
        var secretKey  = new SecretKey(apiKey, apiSecret);

        services.AddSingleton<IAlpacaTradingClient>(_ => env.GetAlpacaTradingClient(secretKey));
        services.AddSingleton<IAlpacaDataClient>(_ => env.GetAlpacaDataClient(secretKey));
        services.AddSingleton<IAlpacaOptionsDataClient>(_ => env.GetAlpacaOptionsDataClient(secretKey));

        // Persists ReversalCallFunction's open-position state (entry premium,
        // peak premium, trailing-stop arm flag) to Azure Table Storage so it
        // survives host restarts / cold starts.
        services.AddSingleton<ReversalPositionStore>();
    })
    .ConfigureLogging(logging =>
    {
        logging.SetMinimumLevel(LogLevel.Information);
    })
    .Build();

host.Run();
