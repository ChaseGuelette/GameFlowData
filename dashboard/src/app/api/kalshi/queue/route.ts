import { NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { createAdminClient } from '@/lib/supabase/admin'

export async function GET() {
  // Auth check
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  // Use admin client to read trade queue (no RLS on this table)
  const admin = createAdminClient()

  const { data: pending, error: pendingError } = await admin
    .from('kalshi_trade_queue')
    .select('*')
    .eq('status', 'pending_approval')
    .gt('expires_at', new Date().toISOString())
    .order('proposed_at', { ascending: false })

  if (pendingError) {
    return NextResponse.json({ error: pendingError.message }, { status: 500 })
  }

  const { data: failed, error: failedError } = await admin
    .from('kalshi_trade_queue')
    .select('*')
    .eq('status', 'failed')
    .gt('proposed_at', new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
    .order('proposed_at', { ascending: false })

  if (failedError) {
    return NextResponse.json({ error: failedError.message }, { status: 500 })
  }

  return NextResponse.json({ trades: [...(pending ?? []), ...(failed ?? [])] })
}