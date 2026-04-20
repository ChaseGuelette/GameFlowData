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
  const { data, error } = await admin
    .from('kalshi_trade_queue')
    .select('*')
    .eq('status', 'pending_approval')
    .gt('expires_at', new Date().toISOString())
    .order('proposed_at', { ascending: false })

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }

  return NextResponse.json({ trades: data ?? [] })
}
