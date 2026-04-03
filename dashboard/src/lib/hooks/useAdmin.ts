'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'

async function checkIsAdmin(): Promise<boolean> {
  const supabase = createClient()
  const { data, error } = await supabase.rpc('is_admin')
  if (error) return false
  return data === true
}

export function useAdmin() {
  const { data: isAdmin = false, isLoading } = useQuery({
    queryKey: ['admin-check'],
    queryFn: checkIsAdmin,
    staleTime: 30 * 60 * 1000, // 30 minutes
    gcTime: 60 * 60 * 1000,
  })

  return { isAdmin, isLoading }
}
