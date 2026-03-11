import { createBrowserClient } from '@supabase/ssr'

export function createClient() {
  // Use placeholder values during build-time prerendering (env vars unavailable).
  // The client is only used at runtime in the browser where env vars are injected.
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || 'https://placeholder.supabase.co'
  const key = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || 'placeholder-key'
  return createBrowserClient(url, key)
}
