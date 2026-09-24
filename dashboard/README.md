# GameFlow dashboard

Status: active source-backed setup guide. Reconciled 2026-08-24.

The retained Next.js application is the customer-facing NBA/MLB dashboard. It uses the App Router, Supabase authentication/RLS, server/API routes, and optional Stripe subscription flows.

## Requirements

- Node.js `>=20.9.0` (the engine required by the locked Next.js 16.1.6 package)
- npm and the committed `package-lock.json`
- Supabase client configuration for real authenticated data

Install and validate from `dashboard/`:

```powershell
npm ci
npm run dev
npm run lint
npm run build
```

Open `http://localhost:3000` for local development.

## Environment

Core Supabase client variables:

- `NEXT_PUBLIC_SUPABASE_URL`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY`

Server-only/optional product variables:

- `SUPABASE_SERVICE_ROLE_KEY`
- `SUBSCRIPTION_REQUIRED`
- `NEXT_PUBLIC_SITE_URL`
- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `STRIPE_PRICE_MONTHLY`
- `STRIPE_PRICE_ANNUAL`
- `NEXT_PUBLIC_STRIPE_PRICE_MONTHLY`
- `NEXT_PUBLIC_STRIPE_PRICE_ANNUAL`

Never expose the service-role or Stripe secret/webhook values to browser code. Placeholder Supabase values permit build-time rendering but do not prove an authenticated runtime works.

## Retained pages

Public/authenticated route groups currently provide:

- landing, public picks, privacy, and terms;
- login and signup;
- dashboard, DFS, combined performance/history, account, and subscription pages.

There is no standalone `/history`, `/pricing`, Bot Tracker, Arb Scanner, Data Vault, Kalshi, Polymarket, or NCAAB page in the reduced tree.

## Retained API surface

- Ask AI and Ask history
- games and scoreboard
- slate
- Stripe checkout, portal, and webhook
- authentication callback

The production build route listing is the final route authority.

## Data and authorization boundary

- Browser/dashboard queries use the authenticated Supabase client and RLS.
- Service-role access is restricted to server-only code paths that need it.
- Python backend jobs use their separate backend database role; do not copy that connection into dashboard variables.
- Subscription enforcement activates only when `SUBSCRIPTION_REQUIRED=true`.

## Validation

A clean dashboard verification is:

```powershell
npm ci
npm run lint
npm run build
```

Lint warnings must be reported even when the command exits successfully. A successful static build does not replace end-to-end validation of Supabase auth, Stripe webhooks, protected navigation, or real API data.

## Deployment

Vercel is the intended dashboard host. The supported Vercel project root is `dashboard`, so `dashboard/vercel.json` is the applicable configuration; the repository-root `vercel.json` is not used by that setup. Do not treat the generic Next.js “Deploy” flow as sufficient: set the required environment variables and verify the protected/authenticated path after deployment.

The Railway worker documented in `../docs/railway_deployment.md` is a separate Python service and does not host this Next.js application.

## Related documentation

- [Project README](../README.md)
- [Documentation index](../docs/README.md)
- [Railway worker](../docs/railway_deployment.md)
