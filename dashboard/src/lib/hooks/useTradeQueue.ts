'use client'

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import type { KalshiTradeQueueItem } from '@/types/bot-tracker'

async function fetchTradeQueue(): Promise<KalshiTradeQueueItem[]> {
  const res = await fetch('/api/kalshi/queue')
  if (!res.ok) throw new Error('Failed to fetch trade queue')
  const data = await res.json()
  return data.trades ?? []
}

interface ApproveResult {
  success: boolean
  action: string
  count: number
  expired?: number
  trade_ids?: number[]
  message?: string
}

async function approveAction(
  action: 'approve' | 'reject' | 'approve_all' | 'retry' | 'dismiss' | 'dismiss_all',
  tradeIds: number[] = [],
): Promise<ApproveResult> {
  const res = await fetch('/api/kalshi/approve', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ trade_ids: tradeIds, action }),
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: 'Request failed' }))
    throw new Error(err.error ?? 'Approval failed')
  }
  return res.json()
}

export function useTradeQueue() {
  return useQuery({
    queryKey: ['trade-queue'],
    queryFn: fetchTradeQueue,
    staleTime: 15 * 1000,
    refetchInterval: 15 * 1000,
  })
}

export function useTradeApproval() {
  const queryClient = useQueryClient()

  const approve = useMutation({
    mutationFn: (tradeIds: number[]) => approveAction('approve', tradeIds),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['trade-queue'] })
      queryClient.invalidateQueries({ queryKey: ['bot-tracker'] })
    },
  })

  const reject = useMutation({
    mutationFn: (tradeIds: number[]) => approveAction('reject', tradeIds),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['trade-queue'] })
    },
  })

  const approveAll = useMutation({
    mutationFn: () => approveAction('approve_all'),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['trade-queue'] })
      queryClient.invalidateQueries({ queryKey: ['bot-tracker'] })
    },
  })

  const retry = useMutation({
    mutationFn: (tradeIds: number[]) => approveAction('retry', tradeIds),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['trade-queue'] })
      queryClient.invalidateQueries({ queryKey: ['bot-tracker'] })
    },
  })

  const dismiss = useMutation({
    mutationFn: (tradeIds: number[]) => approveAction('dismiss', tradeIds),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['trade-queue'] }),
  })

  const dismissAll = useMutation({
    mutationFn: () => approveAction('dismiss_all'),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['trade-queue'] }),
  })

  return { approve, reject, approveAll, retry, dismiss, dismissAll }
}
