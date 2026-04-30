import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { createAdminClient } from '@/lib/supabase/admin'

interface CancelApproveBody {
  order_ids?: number[]
  action: 'approve' | 'reject' | 'approve_all'
}

export async function POST(request: NextRequest) {
  // Auth check
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  let body: CancelApproveBody
  try {
    body = await request.json()
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 })
  }

  const { action } = body
  if (!action || !['approve', 'reject', 'approve_all'].includes(action)) {
    return NextResponse.json({ error: 'Invalid action' }, { status: 400 })
  }

  const admin = createAdminClient()
  const now = new Date().toISOString()

  if (action === 'reject') {
    if (!body.order_ids?.length) {
      return NextResponse.json({ error: 'order_ids required for reject' }, { status: 400 })
    }
    const { error } = await admin
      .from('kalshi_cancel_queue')
      .update({ status: 'rejected' })
      .in('id', body.order_ids)
      .eq('status', 'pending_review')

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 })
    }
    return NextResponse.json({ success: true, action: 'rejected', count: body.order_ids.length })
  }

  // approve or approve_all
  let orderIds: number[] = []

  if (action === 'approve_all') {
    const { data: pending } = await admin
      .from('kalshi_cancel_queue')
      .select('id')
      .eq('status', 'pending_review')

    orderIds = (pending ?? []).map((r: { id: number }) => r.id)
  } else {
    if (!body.order_ids?.length) {
      return NextResponse.json({ error: 'order_ids required for approve' }, { status: 400 })
    }
    orderIds = body.order_ids
  }

  if (!orderIds.length) {
    return NextResponse.json({ success: true, action: 'approved', count: 0, message: 'No pending orders' })
  }

  const { error: approveError } = await admin
    .from('kalshi_cancel_queue')
    .update({ status: 'approved', approved_at: now })
    .in('id', orderIds)
    .eq('status', 'pending_review')

  if (approveError) {
    return NextResponse.json({ error: approveError.message }, { status: 500 })
  }

  return NextResponse.json({ success: true, action: 'approved', count: orderIds.length })
}
