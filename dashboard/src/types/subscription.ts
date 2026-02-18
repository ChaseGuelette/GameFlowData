export type SubscriptionStatus = 'active' | 'inactive' | 'past_due' | 'canceled' | 'trialing'

export interface UserSubscription {
  id: string
  user_id: string
  status: SubscriptionStatus
  plan_id: string
  price_cents: number
  currency: string
  current_period_start: string | null
  current_period_end: string | null
  trial_start: string | null
  trial_end: string | null
  cancel_at_period_end: boolean
  canceled_at: string | null
  created_at: string
  updated_at: string
}

export interface PricingPlan {
  id: string
  name: string
  price_cents: number
  currency: string
  interval: 'month' | 'year'
  features: string[]
}
