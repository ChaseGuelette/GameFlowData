import { NextRequest, NextResponse } from 'next/server'
import type Stripe from 'stripe'
import { getStripe } from '@/lib/stripe'
import { createAdminClient } from '@/lib/supabase/admin'
import type { SubscriptionStatus } from '@/types/subscription'

function mapStripeStatus(stripeStatus: string): SubscriptionStatus {
  switch (stripeStatus) {
    case 'active': return 'active'
    case 'trialing': return 'trialing'
    case 'past_due': return 'past_due'
    case 'canceled': return 'canceled'
    default: return 'inactive'
  }
}

// In Stripe API dahlia (v22), current_period_start/end moved to subscription items
function getPeriodDates(subscription: Stripe.Subscription) {
  const item = subscription.items.data[0] as (Stripe.SubscriptionItem & {
    current_period_start?: number
    current_period_end?: number
  }) | undefined
  return {
    current_period_start: item?.current_period_start != null
      ? new Date(item.current_period_start * 1000).toISOString()
      : null,
    current_period_end: item?.current_period_end != null
      ? new Date(item.current_period_end * 1000).toISOString()
      : null,
  }
}

export async function POST(request: NextRequest) {
  const body = await request.text()
  const signature = request.headers.get('stripe-signature')

  if (!signature) {
    return NextResponse.json({ error: 'Missing stripe-signature' }, { status: 400 })
  }

  const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET
  if (!webhookSecret) {
    console.error('STRIPE_WEBHOOK_SECRET is not set')
    return NextResponse.json({ error: 'Webhook secret not configured' }, { status: 500 })
  }

  let event: Stripe.Event
  try {
    const stripe = getStripe()
    event = stripe.webhooks.constructEvent(body, signature, webhookSecret)
  } catch (err) {
    console.error('Webhook signature verification failed:', err)
    return NextResponse.json({ error: 'Invalid signature' }, { status: 400 })
  }

  const supabase = createAdminClient()

  try {
    switch (event.type) {
      case 'checkout.session.completed': {
        const session = event.data.object as Stripe.Checkout.Session
        const userId = session.metadata?.user_id
        if (!userId || !session.subscription) break

        const stripe = getStripe()
        const subscription = await stripe.subscriptions.retrieve(session.subscription as string)
        const { current_period_start, current_period_end } = getPeriodDates(subscription)

        const { error } = await supabase
          .from('user_subscriptions')
          .upsert({
            user_id: userId,
            stripe_customer_id: session.customer as string,
            stripe_subscription_id: subscription.id,
            status: mapStripeStatus(subscription.status),
            plan_id: subscription.items.data[0]?.price.id ?? '',
            current_period_start,
            current_period_end,
            trial_start: subscription.trial_start
              ? new Date(subscription.trial_start * 1000).toISOString()
              : null,
            trial_end: subscription.trial_end
              ? new Date(subscription.trial_end * 1000).toISOString()
              : null,
            cancel_at_period_end: subscription.cancel_at_period_end,
            updated_at: new Date().toISOString(),
          }, { onConflict: 'user_id' })

        if (error) console.error('checkout.session.completed upsert error:', error)
        break
      }

      case 'customer.subscription.updated': {
        const subscription = event.data.object as Stripe.Subscription
        const { current_period_start, current_period_end } = getPeriodDates(subscription)

        const { error } = await supabase
          .from('user_subscriptions')
          .update({
            status: mapStripeStatus(subscription.status),
            plan_id: subscription.items.data[0]?.price.id ?? '',
            current_period_start,
            current_period_end,
            trial_start: subscription.trial_start
              ? new Date(subscription.trial_start * 1000).toISOString()
              : null,
            trial_end: subscription.trial_end
              ? new Date(subscription.trial_end * 1000).toISOString()
              : null,
            cancel_at_period_end: subscription.cancel_at_period_end,
            updated_at: new Date().toISOString(),
          })
          .eq('stripe_subscription_id', subscription.id)

        if (error) console.error('customer.subscription.updated error:', error)
        break
      }

      case 'customer.subscription.deleted': {
        const subscription = event.data.object as Stripe.Subscription

        const { error } = await supabase
          .from('user_subscriptions')
          .update({
            status: 'canceled',
            canceled_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          })
          .eq('stripe_subscription_id', subscription.id)

        if (error) console.error('customer.subscription.deleted error:', error)
        break
      }

      case 'invoice.payment_failed': {
        const invoice = event.data.object as Stripe.Invoice
        // In Stripe API dahlia (v22), subscription moved to invoice.parent.subscription_details.subscription
        const subscriptionId = invoice.parent?.subscription_details?.subscription
        if (!subscriptionId) break
        const subId = typeof subscriptionId === 'string' ? subscriptionId : subscriptionId.id

        const { error } = await supabase
          .from('user_subscriptions')
          .update({
            status: 'past_due',
            updated_at: new Date().toISOString(),
          })
          .eq('stripe_subscription_id', subId)

        if (error) console.error('invoice.payment_failed error:', error)
        break
      }
    }
  } catch (err) {
    console.error('Webhook handler error:', err)
    return NextResponse.json({ error: 'Handler error' }, { status: 500 })
  }

  return NextResponse.json({ received: true })
}
