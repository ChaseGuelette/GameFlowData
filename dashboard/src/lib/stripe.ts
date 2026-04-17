import Stripe from 'stripe'

let stripeInstance: Stripe | null = null

export function getStripe(): Stripe {
  if (!stripeInstance) {
    if (!process.env.STRIPE_SECRET_KEY) {
      throw new Error('STRIPE_SECRET_KEY is not set')
    }
    stripeInstance = new Stripe(process.env.STRIPE_SECRET_KEY)
  }
  return stripeInstance
}

export const STRIPE_PRICE_IDS = {
  pro_monthly: process.env.STRIPE_PRICE_MONTHLY ?? '',
  pro_annual: process.env.STRIPE_PRICE_ANNUAL ?? '',
} as const

export type PlanInterval = keyof typeof STRIPE_PRICE_IDS

export const TRIAL_PERIOD_DAYS = 7
