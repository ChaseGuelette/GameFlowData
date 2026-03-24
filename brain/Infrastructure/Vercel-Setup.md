# Vercel Setup

> Part of [[Infrastructure]]

## Architecture
- Next.js 16 App Router
- Root directory: `dashboard/` (configured in root `vercel.json`)
- URL: `game-flow-data.vercel.app`

## Environment Variables
- `NEXT_PUBLIC_SUPABASE_URL` — Supabase project URL
- `NEXT_PUBLIC_SUPABASE_ANON_KEY` — Supabase anon key
- `ANTHROPIC_API_KEY` — For AI Q&A chat feature

## Security Headers (Session 79)
Added to `next.config.ts`:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-XSS-Protection`
- `Referrer-Policy`
- `Permissions-Policy`

## Error Boundaries
`error.tsx` in all 3 route groups (`(protected)`, `(public)`, `(auth)`). Catches runtime errors and shows retry UI.

## Current Status
Deploy is up to date as of March 24, 2026. All features pushed including AI Q&A, combo markets, DFS 6-stat, mobile optimization, security headers, error boundaries.

#vercel #infrastructure #deployment
