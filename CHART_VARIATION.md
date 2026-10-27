# Five chart views

Replace `square_desk/images.py` and add `square_desk/chart_views.py` in the same directory. Commit both files and redeploy the existing Render service. No ENV changes or new packages are needed.

The renderer selects one of five views from the coin symbol and the closed-candle timestamp. The same evidence selects the same view; different candle time slots rotate the selection without database state or randomness. Consecutive publications are not guaranteed to use different views because their coins and timestamps can differ.

| View | Additional information |
| --- | --- |
| Trend | EMA20 and EMA50 |
| Value | Rolling 96-bar VWAP and EMA20 |
| Volatility | Bollinger bands: 20-bar SMA and two population standard deviations |
| Momentum | EMA20 plus a separate MACD(12,26,9) line, signal and histogram panel |
| Structure | Existing prior 20-bar support/resistance, ATR-sized reference zones and range midpoint |

Every view retains genuine closed OHLC candles, base-asset volume, support/resistance, source and candle-close timestamp. RSI(14) has its own pane with 30/50/70 references, using the same Wilder calculation as the existing analysis. Indicators use the full supplied snapshot history before the last 70 candles are plotted. VWAP is omitted when fewer than 96 closed bars or zero total volume make it unavailable. Bollinger bands describe dispersion, not guaranteed support/resistance.

Only rendering changes. Service, analysis, content, scheduler, publisher, approval gates, image queue/caps and timeouts are byte-identical to the previous fast-sync release. There are no additional API calls, database queries or worker threads. Existing draft charts retain their reviewed content hashes; newly rendered charts receive new content-addressed PNG names. Editorial cards remain unchanged.

Open interest is not inferred from spot candles or volume. The rendering interface can show a matching, recent, explicitly supplied Binance USDT-perpetual OI value with its separate timestamp. The current unchanged service does not supply this metadata, and this patch does not enable derivatives collection. Therefore OI is not active on current charts. Enabling an actual OI data feed is separate work.

The output is 1600 x 1100 PNG, within the existing 2000-pixel and 1,000,000-byte media limits. Five clearly labeled synthetic layout samples measured 77,284-87,254 bytes and 0.141-0.214 seconds locally. These are design previews, not live market data or a Render performance forecast. No change guarantees perpetual usage below a cloud free-tier bandwidth quota.

The complete ZIP also includes the previous guarded Aiven audit repair and bounded background replica batching. The two-file chart patch is for an otherwise already updated deployment and does not install database fixes.
