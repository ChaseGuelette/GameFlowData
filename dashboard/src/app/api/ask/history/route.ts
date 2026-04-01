import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { type ChatHistoryResponse } from '@/types/chat'

export async function GET(request: NextRequest) {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  const { searchParams } = new URL(request.url)
  const playerId = searchParams.get('player_id')
  const stat = searchParams.get('stat')
  const gameDate = searchParams.get('game_date')

  if (!playerId || !stat || !gameDate) {
    return NextResponse.json(
      { error: 'Missing required params: player_id, stat, game_date' },
      { status: 400 }
    )
  }

  // Look up conversation
  const { data: conv } = await supabase
    .from('chat_conversations')
    .select('id')
    .eq('user_id', user.id)
    .eq('player_id', parseInt(playerId, 10))
    .eq('stat', stat)
    .eq('game_date', gameDate)
    .single()

  if (!conv) {
    const response: ChatHistoryResponse = { conversation_id: null, messages: [] }
    return NextResponse.json(response)
  }

  // Load messages
  const { data: msgs } = await supabase
    .from('chat_messages')
    .select('role, content')
    .eq('conversation_id', conv.id)
    .order('created_at', { ascending: true })
    .limit(50)

  const response: ChatHistoryResponse = {
    conversation_id: conv.id,
    messages: (msgs || []).map(m => ({ role: m.role as 'user' | 'assistant', content: m.content })),
  }

  return NextResponse.json(response)
}

export async function DELETE(request: NextRequest) {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  const { searchParams } = new URL(request.url)
  const conversationId = searchParams.get('conversation_id')

  if (!conversationId) {
    return NextResponse.json({ error: 'Missing conversation_id' }, { status: 400 })
  }

  // Delete conversation (cascade deletes messages)
  await supabase
    .from('chat_conversations')
    .delete()
    .eq('id', parseInt(conversationId, 10))
    .eq('user_id', user.id)

  return NextResponse.json({ success: true })
}
