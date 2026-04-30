import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { createAdminClient } from '@/lib/supabase/admin'

interface ApproveBody {
  trade_ids: number[]
  action: 'approve' | 'reject' | 'approve_all' | 'retry' | 'dismiss' | 'dismiss_all'
}

export async function POST(request: NextRequest) {
  // Auth check
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  let body: ApproveBody
  try {
    body = await request.json()
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 })
  }

  const { action } = body
  if (!action || !['approve', 'reject', 'approve_all', 'retry', 'dismiss', 'dismiss_all'].includes(action)) {
    return NextResponse.json({ error: 'Invalid action' }, { status: 400 })
  }

  const admin = createAdminClient()
  const now = new Date().toISOString()

  if (action === 'dismiss') {
    if (!body.trade_ids?.length) {
      return NextResponse.json({ error: 'trade_ids required for dismiss' }, { status: 400 })
    }
    const { error } = await admin
      .from('kalshi_trade_queue')
      .update({ status: 'dismissed' })
      .in('id', body.trade_ids)
      .eq('status', 'failed')
    if (error) return NextResponse.json({ error: error.message }, { status: 500 })
    return NextResponse.json({ success: true, action: 'dismissed', count: body.trade_ids.length })
  }

  if (action === 'dismiss_all') {
    const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
    const { data: failed } = await admin
      .from('kalshi_trade_queue')
      .select('id')
      .eq('status', 'failed')
      .gte('proposed_at', cutoff)
    const ids = (failed ?? []).map((t: { id: number }) => t.id)
    if (!ids.length) return NextResponse.json({ success: true, action: 'dismissed', count: 0 })
    const { error } = await admin
      .from('kalshi_trade_queue')
      .update({ status: 'dismissed' })
      .in('id', ids)
    if (error) return NextResponse.json({ error: error.message }, { status: 500 })
    return NextResponse.json({ success: true, action: 'dismissed', count: ids.length })
  }

  if (action === 'retry') {
    if (!body.trade_ids?.length) {
      return NextResponse.json({ error: 'trade_ids required for retry' }, { status: 400 })
    }

    const { data: validTrades, error: fetchError } = await admin
      .from('kalshi_trade_queue')
      .select('id')
      .in('id', body.trade_ids)
      .eq('status', 'failed')

    if (fetchError) {
      return NextResponse.json({ error: fetchError.message }, { status: 500 })
    }

    const validIds = (validTrades ?? []).map((t: { id: number }) => t.id)
    if (!validIds.length) {
      return NextResponse.json({ error: 'No failed trades found for retry' }, { status: 404 })
    }

    const refreshedExpiry = new Date(Date.now() + 30 * 60 * 1000).toISOString()
    const { error: updateError } = await admin
      .from('kalshi_trade_queue')
      .update({ status: 'approved', approved_at: now, expires_at: refreshedExpiry })
      .in('id', validIds)

    if (updateError) {
      return NextResponse.json({ error: updateError.message }, { status: 500 })
    }

    return NextResponse.json({ success: true, action: 'retry', count: validIds.length, trade_ids: validIds })
  }

  if (action === 'reject') {
    if (!body.trade_ids?.length) {
      return NextResponse.json({ error: 'trade_ids required for reject' }, { status: 400 })
    }
    const { error } = await admin
      .from('kalshi_trade_queue')
      .update({ status: 'rejected' })
      .in('id', body.trade_ids)
      .eq('status', 'pending_approval')

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 })
    }
    return NextResponse.json({ success: true, action: 'rejected', count: body.trade_ids.length })
  }

  // For approve / approve_all: get the trade IDs
  let tradeIds: number[] = []

  if (action === 'approve_all') {
    const { data: pending } = await admin
      .from('kalshi_trade_queue')
      .select('id')
      .eq('status', 'pending_approval')
      .gt('expires_at', now)

    tradeIds = (pending ?? []).map((t: { id: number }) => t.id)
  } else {
    if (!body.trade_ids?.length) {
      return NextResponse.json({ error: 'trade_ids required for approve' }, { status: 400 })
    }
    tradeIds = body.trade_ids
  }

  if (!tradeIds.length) {
    return NextResponse.json({ success: true, action: 'approved', count: 0, message: 'No pending trades' })
  }

  // Check for expired trades
  const { data: validTrades } = await admin
    .from('kalshi_trade_queue')
    .select('id, expires_at')
    .in('id', tradeIds)
    .eq('status', 'pending_approval')

  const expired: number[] = []
  const valid: number[] = []

  for (const trade of validTrades ?? []) {
    if (new Date(trade.expires_at) < new Date()) {
      expired.push(trade.id)
    } else {
      valid.push(trade.id)
    }
  }

  // Mark expired trades
  if (expired.length > 0) {
    await admin
      .from('kalshi_trade_queue')
      .update({ status: 'expired' })
      .in('id', expired)
  }

  if (!valid.length) {
    return NextResponse.json({
      success: true,
      action: 'approved',
      count: 0,
      expired: expired.length,
      message: 'All trades expired',
    })
  }

  // Mark as approved with a fresh 30-min expiry window so the execute-approved
  // cron has time to place the order regardless of how close to the original
  // expiry the human approved.
  const refreshedExpiry = new Date(Date.now() + 30 * 60 * 1000).toISOString()
  const { error: approveError } = await admin
    .from('kalshi_trade_queue')
    .update({ status: 'approved', approved_at: now, expires_at: refreshedExpiry })
    .in('id', valid)

  if (approveError) {
    return NextResponse.json({ error: approveError.message }, { status: 500 })
  }

  // Trigger execution via Edge Function or direct API call
  // For now, call the execute endpoint which the Python backend exposes
  // The trades are marked 'approved' — the next refresh cycle will pick them up
  // OR we can trigger immediate execution via an edge function

  return NextResponse.json({
    success: true,
    action: 'approved',
    count: valid.length,
    expired: expired.length,
    trade_ids: valid,
  })
}
