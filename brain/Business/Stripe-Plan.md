# Stripe Integration Plan

> Part of [[Business]]

## Status: TODO (deferred since Session 42)

### What Needs to Be Built

#### Database Changes
- Add `stripe_customer_id` column to `user_subscriptions`
- Add `stripe_subscription_id` column to `user_subscriptions`

#### Frontend Pages
- `/subscribe` — Stripe Checkout session creation and redirect
- `/account` — Stripe Customer Portal link for self-service billing

#### API Route
- `dashboard/src/app/api/stripe/webhook/route.ts` — Webhook handler for:
  - `checkout.session.completed` → create subscription record
  - `customer.subscription.updated` → update status/period
  - `customer.subscription.deleted` → mark inactive
  - `invoice.payment_failed` → handle failed payments

#### Existing Infrastructure
- `user_subscriptions` table exists with RLS (users can view own sub)
- `is_subscribed(uuid)` function checks active/trialing + period_end
- Middleware already checks subscription status for protected routes
- Subscription-exempt routes: `/subscribe`, `/account`
- Prediction tables already have RLS requiring active subscription

### Integration Points
The subscription check is already wired into middleware and RLS. Once Stripe populates `user_subscriptions`, the paywall activates automatically.

#stripe #business #monetization
