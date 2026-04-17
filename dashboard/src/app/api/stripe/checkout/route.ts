import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { getStripe, STRIPE_PRICE_IDS, TRIAL_PERIOD_DAYS, type PlanInterval } from '@/lib/stripe'

export async function POST(request: NextRequest) {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  let body: { plan: PlanInterval }
  try {
    body = await request.json()
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 })
  }

  const { plan } = body
  if (!plan || !(plan in STRIPE_PRICE_IDS)) {
    return NextResponse.json({ error: 'Invalid plan' }, { status: 400 })
  }

  const priceId = STRIPE_PRICE_IDS[plan]
  if (!priceId) {
    return NextResponse.json({ error: 'Price not configured' }, { status: 500 })
  }

  const stripe = getStripe()

  // Get or create Stripe customer
  const { data: subData } = await supabase
    .from('user_subscriptions')
    .select('stripe_customer_id')
    .eq('user_id', user.id)
    .single()

  let customerId = subData?.stripe_customer_id ?? null

  if (!customerId) {
    const customer = await stripe.customers.create({
      email: user.email,
      metadata: { user_id: user.id },
    })
    customerId = customer.id
  }

  // Trial abuse prevention: skip trial if customer has prior subscription
  let trialPeriodDays: number | undefined = TRIAL_PERIOD_DAYS
  const existingSubs = await stripe.subscriptions.list({
    customer: customerId,
    limit: 1,
  })
  if (existingSubs.data.length > 0) {
    trialPeriodDays = undefined
  }

  const origin = request.headers.get('origin') ?? process.env.NEXT_PUBLIC_SITE_URL ?? 'http://localhost:3000'

  const session = await stripe.checkout.sessions.create({
    customer: customerId,
    mode: 'subscription',
    payment_method_types: ['card'],
    line_items: [{ price: priceId, quantity: 1 }],
    subscription_data: {
      trial_period_days: trialPeriodDays,
      metadata: { user_id: user.id },
    },
    metadata: { user_id: user.id },
    success_url: `${origin}/account?checkout=success`,
    cancel_url: `${origin}/subscribe`,
  })

  return NextResponse.json({ url: session.url })
}
