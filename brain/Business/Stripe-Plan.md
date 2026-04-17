# Stripe Integration Plan

> Part of [[Business]]

## Status: COMPLETE (Session 44 — April 17, 2026)

### What Was Built

#### Database
- Migration `add_stripe_columns` applied: `stripe_customer_id TEXT`, `stripe_subscription_id TEXT` + 4 indexes on `user_subscriptions`

#### New Files (5)
- `dashboard/src/lib/stripe.ts` — lazy Stripe singleton, `STRIPE_PRICE_IDS`, `TRIAL_PERIOD_DAYS=7`, `PlanInterval` type
- `dashboard/src/lib/supabase/admin.ts` — service-role Supabase client for server-side writes (bypasses RLS)
- `dashboard/src/app/api/stripe/webhook/route.ts` — handles 4 events (checkout completed, sub updated, sub deleted, payment failed)
- `dashboard/src/app/api/stripe/checkout/route.ts` — creates Checkout Session; trial abuse prevention via `subscriptions.list`
- `dashboard/src/app/api/stripe/portal/route.ts` — creates Customer Portal session

#### Modified Files (6)
- `dashboard/src/types/subscription.ts` — added `stripe_customer_id` + `stripe_subscription_id` fields
- `dashboard/src/lib/subscription.ts` — changed `.select('status, current_period_end')` → `.select('*')`
- `dashboard/src/app/(protected)/subscribe/page.tsx` — replaced redirect stub with full pricing UI (Monthly $19.99 + Annual $199 with Save 17% badge, trial messaging, checkout flow)
- `dashboard/src/app/(protected)/account/page.tsx` — dynamic status badge, plan name, Manage Billing button, Subscribe Now CTA
- `dashboard/src/app/(public)/pricing/page.tsx` — replaced $0 beta card with Monthly + Annual cards, auth-aware CTAs
- `dashboard/src/lib/supabase/middleware.ts` — subscription gate (env-toggled, `SUBSCRIPTION_REQUIRED=true`)

### Pricing
- **Pro Monthly**: $19.99/mo
- **Pro Annual**: $199/yr (saves 17%)
- **Free trial**: 7 days — included for new customers, skipped for returning customers (abuse prevention)

### Paywall State
- `SUBSCRIPTION_REQUIRED=false` (free beta mode)
- Flip to `true` in `.env.local` + Vercel when ready to monetize

### Stripe Dashboard Setup (still needed)
1. Create account at dashboard.stripe.com (start in Test Mode)
2. Create product "GameFlow Pro" with two prices → copy `price_` IDs → env vars
3. Create webhook endpoint (URL: `https://yourdomain.com/api/stripe/webhook`) for 4 events
4. Configure Customer Portal (update payment, cancel, switch plans)
5. Get API keys → `STRIPE_SECRET_KEY`, `NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY`

### Stripe v22 / Dahlia API Notes (CRITICAL for future dev)
- `Subscription.current_period_start/end` REMOVED — now at `subscription.items.data[0].current_period_start/end`
- `Invoice.subscription` REMOVED — now at `invoice.parent.subscription_details.subscription`
- Initialize with `new Stripe(key)` — no `apiVersion` arg needed (defaults to `2026-03-25.dahlia`)

### To Test (once env vars filled in)
```bash
stripe listen --forward-to localhost:3000/api/stripe/webhook
# Test card: 4242 4242 4242 4242
```

#stripe #business #monetization
