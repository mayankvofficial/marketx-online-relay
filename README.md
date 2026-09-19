# MarketX Online Relay

MarketX's public runtime repository for the live market bridge.

## Schedule

The bridge is intended to run Monday-Friday during the MarketX market window:

**09:00 IST → 23:50 IST**

The GitHub Actions workflow splits the session into three jobs because a GitHub-hosted job has a 6-hour maximum runtime:

- Phase 1: 09:00-14:50 IST
- Phase 2: 14:50-20:40 IST
- Phase 3: 20:40-23:50 IST

The phases hand off automatically. The bridge writes ticks directly to Supabase.

## Required repository secret

Add this GitHub Actions secret:

- `SUPABASE_SECRET_KEY`

The secret is never stored in this public repository.

## Current market source

- Trade99 WebSocket endpoint: `https://trade99.live:3000`
- Default symbol: `CRUDEOIL26SEPFUT`
- Supabase table: `public.market_snapshots`

## Important

This repository contains runtime code only. No Supabase secret or private credential belongs in the repository.
