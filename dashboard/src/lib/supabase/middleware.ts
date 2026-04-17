import { createServerClient } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'

const PUBLIC_ROUTES = ['/', '/picks', '/pricing', '/terms', '/privacy']
const AUTH_ROUTES = ['/login', '/signup']
const ADMIN_ROUTES = ['/bot-tracker']

export async function updateSession(request: NextRequest) {
  let supabaseResponse = NextResponse.next({ request })

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        getAll() {
          return request.cookies.getAll()
        },
        setAll(cookiesToSet) {
          cookiesToSet.forEach(({ name, value }) => request.cookies.set(name, value))
          supabaseResponse = NextResponse.next({ request })
          cookiesToSet.forEach(({ name, value, options }) =>
            supabaseResponse.cookies.set(name, value, options)
          )
        },
      },
    }
  )

  // Always refresh session
  const { data: { user } } = await supabase.auth.getUser()

  const pathname = request.nextUrl.pathname

  // 1. Public routes — pass through
  if (PUBLIC_ROUTES.includes(pathname)) {
    return supabaseResponse
  }

  // 2. Auth callback — pass through
  if (pathname.startsWith('/auth')) {
    return supabaseResponse
  }

  // 3. Auth routes (login/signup) — redirect authenticated users to dashboard
  if (AUTH_ROUTES.includes(pathname)) {
    if (user) {
      const url = request.nextUrl.clone()
      url.pathname = '/dashboard'
      return NextResponse.redirect(url)
    }
    return supabaseResponse
  }

  // 4. All remaining routes require authentication
  if (!user) {
    const url = request.nextUrl.clone()
    url.pathname = '/login'
    url.searchParams.set('redirectTo', pathname)
    return NextResponse.redirect(url)
  }

  // 4b. Subscription gate (only when SUBSCRIPTION_REQUIRED=true)
  const SUBSCRIPTION_EXEMPT_ROUTES = ['/subscribe', '/account']
  if (
    process.env.SUBSCRIPTION_REQUIRED === 'true' &&
    !SUBSCRIPTION_EXEMPT_ROUTES.some(route => pathname.startsWith(route))
  ) {
    const { data: isSubscribed } = await supabase.rpc('is_subscribed', { check_user_id: user.id })
    if (!isSubscribed) {
      const url = request.nextUrl.clone()
      url.pathname = '/subscribe'
      return NextResponse.redirect(url)
    }
  }

  // 5. Admin routes — require is_admin() check
  if (ADMIN_ROUTES.some(route => pathname.startsWith(route))) {
    const { data: isAdmin } = await supabase.rpc('is_admin')
    if (!isAdmin) {
      const url = request.nextUrl.clone()
      url.pathname = '/dashboard'
      return NextResponse.redirect(url)
    }
  }

  return supabaseResponse
}
