# Paid Subscription System Implementation Plan

## Overview

Add paid subscription functionality to GameFlowData, allowing users to sign up, subscribe via Stripe, and access predictions. Read-only access for subscribers at **$19.99/month**.

## Current State

**Already Implemented:**
- Supabase Auth (email/password login, middleware protection, session management)
- Protected routes via middleware (redirects to `/login`)
- OAuth callback handler ready
- Dark-themed dashboard with Tailwind

**Not Implemented:**
- Subscription tracking
- Payment processing (Stripe)
- Row-Level Security (RLS) on predictions
- Access control based on subscription status
- Legal pages (ToS, Privacy Policy)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         USER FLOW                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Landing Page ──► 2. Sign Up ──► 3. Subscribe ──► 4. Access  │
│     (public)          (Supabase)     (Stripe)        (dashboard) │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ /        │───►│ /login   │───►│ /pricing │───►│ /         │  │
│  │ landing  │    │ signup   │    │ checkout │    │ dashboard │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│                       │                │               │         │
│                       ▼                ▼               ▼         │
│               ┌──────────────────────────────────────────────┐  │
│               │              SUPABASE                         │  │
│               │  ┌─────────────┐  ┌─────────────────────┐    │  │
│               │  │ auth.users  │  │ user_subscriptions  │    │  │
│               │  └─────────────┘  └─────────────────────┘    │  │
│               │         │                    │                │  │
│               │         ▼                    ▼                │  │
│               │  ┌────────────────────────────────────────┐  │  │
│               │  │ RLS: daily_predictions (subscribers)   │  │  │
│               │  └────────────────────────────────────────┘  │  │
│               └──────────────────────────────────────────────┘  │
│                                                                  │
│                       ┌──────────┐                              │
│                       │  STRIPE  │                              │
│                       │ webhooks │──► Update subscription       │
│                       └──────────┘    status in Supabase        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation Phases

### Phase 1: Database Schema (Supabase)

**Create `user_subscriptions` table:**

```sql
-- User subscription tracking
CREATE TABLE user_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    stripe_customer_id TEXT UNIQUE,
    stripe_subscription_id TEXT UNIQUE,
    subscription_status TEXT NOT NULL DEFAULT 'inactive',
    -- Status values: 'active', 'inactive', 'past_due', 'canceled', 'trialing'
    price_id TEXT,  -- Stripe price ID for the plan
    current_period_start TIMESTAMP WITH TIME ZONE,
    current_period_end TIMESTAMP WITH TIME ZONE,
    cancel_at_period_end BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_id)
);

-- Index for quick lookups
CREATE INDEX idx_user_subscriptions_user_id ON user_subscriptions(user_id);
CREATE INDEX idx_user_subscriptions_stripe_customer ON user_subscriptions(stripe_customer_id);

-- Auto-update timestamp trigger
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER user_subscriptions_updated_at
    BEFORE UPDATE ON user_subscriptions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
```

**Enable RLS on `daily_predictions`:**

```sql
-- Enable RLS
ALTER TABLE daily_predictions ENABLE ROW LEVEL SECURITY;

-- Policy: Active subscribers can view predictions
CREATE POLICY "Subscribers can view predictions"
ON daily_predictions FOR SELECT
USING (
    EXISTS (
        SELECT 1 FROM user_subscriptions
        WHERE user_id = auth.uid()
        AND subscription_status IN ('active', 'trialing')
        AND (current_period_end IS NULL OR current_period_end > NOW())
    )
);

-- Allow service role full access (for backend jobs)
CREATE POLICY "Service role has full access"
ON daily_predictions FOR ALL
USING (auth.role() = 'service_role');
```

---

### Phase 2: Stripe Integration

**Files to Create:**

| File | Purpose |
|------|---------|
| `dashboard/src/lib/stripe.ts` | Stripe client initialization |
| `dashboard/src/app/api/stripe/create-checkout/route.ts` | Create Stripe Checkout session |
| `dashboard/src/app/api/stripe/webhook/route.ts` | Handle Stripe webhooks |
| `dashboard/src/app/api/stripe/portal/route.ts` | Customer portal for subscription management |

**Environment Variables (add to `.env.local`):**
```
STRIPE_SECRET_KEY=sk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRICE_ID=price_...  # Monthly subscription price
NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY=pk_test_...
```

---

### Phase 3: Dashboard UI Changes

**Route Group Structure:**

```
src/app/
├── (public)/                    # No auth required
│   ├── page.tsx                 # Landing page
│   ├── pricing/page.tsx         # Pricing/plans
│   └── layout.tsx               # Public layout
├── (auth)/                      # Auth pages
│   ├── login/page.tsx           # Login
│   └── signup/page.tsx          # Sign up
├── (protected)/                 # Auth + subscription required
│   ├── page.tsx                 # Dashboard (predictions)
│   ├── history/page.tsx         # Betting history
│   ├── performance/page.tsx     # Performance
│   └── layout.tsx               # Protected layout with navbar
├── success/page.tsx             # Post-checkout
├── api/stripe/                  # Stripe endpoints
└── auth/callback/route.ts       # OAuth callback
```

---

### Phase 4: Middleware Enhancement

Update middleware to check subscription status for protected routes.

---

### Phase 5: Legal Pages

| File | Purpose |
|------|---------|
| `dashboard/src/app/(public)/terms/page.tsx` | Terms of Service |
| `dashboard/src/app/(public)/privacy/page.tsx` | Privacy Policy |

**Key Legal Points:**
- Service description (sports predictions, entertainment only)
- Disclaimer: Not financial/gambling advice
- No guarantee of accuracy
- Subscription terms, billing, cancellation

---

### Phase 6: Landing Page

Public landing page with:
- Hero section
- Feature highlights
- Pricing preview
- CTA buttons

---

## Files Summary

### New Files to Create

| File | Purpose |
|------|---------|
| `dashboard/src/lib/stripe.ts` | Stripe client |
| `dashboard/src/app/api/stripe/create-checkout/route.ts` | Checkout session |
| `dashboard/src/app/api/stripe/webhook/route.ts` | Webhook handler |
| `dashboard/src/app/api/stripe/portal/route.ts` | Customer portal |
| `dashboard/src/app/(public)/page.tsx` | Landing page |
| `dashboard/src/app/(public)/pricing/page.tsx` | Pricing page |
| `dashboard/src/app/(public)/terms/page.tsx` | Terms of Service |
| `dashboard/src/app/(public)/privacy/page.tsx` | Privacy Policy |
| `dashboard/src/app/success/page.tsx` | Checkout success |
| `dashboard/src/components/SubscriptionGate.tsx` | Access control |
| `dashboard/src/components/shared/UpgradePrompt.tsx` | Upgrade CTA |

### Files to Modify

| File | Changes |
|------|---------|
| `dashboard/src/lib/supabase/middleware.ts` | Add subscription checks |
| `dashboard/src/middleware.ts` | Update route matcher |
| `dashboard/.env.local` | Add Stripe keys |
| `dashboard/package.json` | Add `stripe` dependency |

---

## Stripe Setup (Manual Steps)

1. **Create Stripe Account** at stripe.com
2. **Create Product** in Stripe Dashboard:
   - Name: "GameFlowData Pro"
   - Price: $19.99/month (recurring)
   - Copy the `price_id`
3. **Get API Keys** from Developers > API Keys
4. **Create Webhook** in Developers > Webhooks:
   - Endpoint: `https://your-domain.com/api/stripe/webhook`
   - Events: `checkout.session.completed`, `customer.subscription.*`, `invoice.payment_failed`
5. **Copy Webhook Secret** (`whsec_...`)

---

## Verification Plan

### Local Testing

1. **Database:** Run migrations in Supabase SQL editor
2. **Stripe:** Use Stripe CLI: `stripe listen --forward-to localhost:3000/api/stripe/webhook`
3. **Test checkout** with card `4242 4242 4242 4242`
4. **Verify access** granted after payment

### Production Checklist

- [ ] Stripe webhook endpoint verified
- [ ] Production Stripe keys in Vercel
- [ ] RLS policies enabled
- [ ] Legal pages reviewed
- [ ] Test end-to-end purchase flow

---

## Implementation Order

| Phase | Tasks | Dependencies |
|-------|-------|--------------|
| 1 | Database schema + RLS | None |
| 2 | Stripe integration (checkout, webhooks) | Phase 1 |
| 3 | Pricing page + checkout button | Phase 2 |
| 4 | Middleware + access control | Phase 1 |
| 5 | Landing page | None |
| 6 | Legal pages | None |
| 7 | Success page + portal | Phase 2 |
| 8 | Testing + deployment | All |

---

## Pricing

| Tier | Price | Features |
|------|-------|----------|
| Free | $0 | Landing page only, no predictions |
| Pro | **$19.99/month** | Full prediction access |

**Future Options:**
- Annual plan: $199/year (2 months free)
- 7-day free trial
- Referral program
