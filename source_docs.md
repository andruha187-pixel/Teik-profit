# External source references used for PRE-JUMP LAB v1

These are documentation references for the public market-data adapters implemented in the bot.

## Binance USD-M Futures

- Public WebSocket catalog:
  https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public
- 2026 WebSocket architecture announcement / migration to `/public` high-frequency data:
  https://www.binance.com/en/support/announcement/detail/ebf9b0aa9eca4ff3804eef6fb09ba32a

Implemented concepts: aggregate trades and 20-level partial book depth at 100 ms on the `/public` endpoint.

## Bybit

- WebSocket connection endpoints:
  https://bybit-exchange.github.io/docs/v5/ws/connect
- Public orderbook:
  https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- Public trades:
  https://bybit-exchange.github.io/docs/v5/websocket/public/trade
- All liquidations:
  https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation

Implemented concepts: 50-level linear orderbook, real-time public trades, all-liquidation stream.

## Coinbase Advanced Trade

- WebSocket overview:
  https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview
- WebSocket channels:
  https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/websocket/websocket-channels

Implemented concepts: public `level2`, `market_trades`, and `heartbeats` subscriptions.

## Polymarket

- Polymarket developer documentation:
  https://docs.polymarket.com/

The lab continues to use the Polymarket market WebSocket/orderbook as the source for outcome prices and executable PAPER fills.

## Chainlink Data Streams

- Chainlink Data Streams documentation:
  https://docs.chain.link/data-streams

Direct Chainlink Data Streams access is deliberately not included in v1. The lab uses an exchange-median start proxy and labels it explicitly as a proxy, not Chainlink.
