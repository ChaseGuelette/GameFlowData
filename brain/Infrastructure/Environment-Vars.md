# Environment Variables

> Part of [[Infrastructure]]

## Railway (Python Backend)
| Variable | Purpose | Required |
|----------|---------|----------|
| `DATABASE_URL` | Supabase PostgreSQL connection | Yes |
| `ODDS_API_KEY` | The Odds API | Yes |
| `RAPIDAPI_KEY` | RapidAPI (injuries) | Yes |
| `DISCORD_CHANNEL_ALERTS` | Discord webhook URL | Yes |

## Vercel (Dashboard)
| Variable | Purpose | Required |
|----------|---------|----------|
| `NEXT_PUBLIC_SUPABASE_URL` | Supabase project URL | Yes |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Supabase anonymous key | Yes |
| `ANTHROPIC_API_KEY` | Claude Haiku for AI Q&A | Yes |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key for webhook writes | Yes (Stripe) |
| `STRIPE_SECRET_KEY` | Stripe secret key (`sk_test_...`) | Yes (Stripe) |
| `NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY` | Stripe publishable key (`pk_test_...`) | Yes (Stripe) |
| `STRIPE_WEBHOOK_SECRET` | Webhook signing secret (`whsec_...`) | Yes (Stripe) |
| `STRIPE_PRICE_MONTHLY` | Monthly price ID (`price_...`) | Yes (Stripe) |
| `STRIPE_PRICE_ANNUAL` | Annual price ID (`price_...`) | Yes (Stripe) |
| `SUBSCRIPTION_REQUIRED` | `"true"` to enforce paywall, `"false"` for free beta | Yes (Stripe) |

## Local Development
| Variable | Purpose | Required |
|----------|---------|----------|
| `DATABASE_URL` | Same as Railway | Yes |
| `ODDS_API_KEY` | Same as Railway | For scraping |
| `RAPIDAPI_KEY` | Same as Railway | For injuries |

## Notes
- Python DB client uses lazy initialization — safely importable without `DATABASE_URL` (for CI/test)
- `ANTHROPIC_API_KEY` must be set on Vercel for AI Q&A to work
- All keys are stored in Railway/Vercel environment settings, not in code

#env-vars #infrastructure #secrets
