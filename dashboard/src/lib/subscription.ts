import { createClient } from '@/lib/supabase/server'
import type { UserSubscription } from '@/types/subscription'

export async function getUserSubscription(userId: string): Promise<UserSubscription | null> {
  const supabase = await createClient()
  const { data, error } = await supabase
    .from('user_subscriptions')
    .select('*')
    .eq('user_id', userId)
    .single()

  if (error || !data) return null
  return data as UserSubscription
}

export function isSubscriptionActive(subscription: UserSubscription | null): boolean {
  if (!subscription) return false
  if (!['active', 'trialing'].includes(subscription.status)) return false
  if (subscription.current_period_end && new Date(subscription.current_period_end) <= new Date()) {
    return false
  }
  return true
}
