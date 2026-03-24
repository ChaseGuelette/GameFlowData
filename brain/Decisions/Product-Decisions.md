# Product Decisions

> Part of [[Decisions]]

## Free Beta Model
No paywall during beta. Public `/picks` shows 3 real picks as teaser. Full access on sign-up. Stripe integration deferred to focus on model accuracy and pipeline reliability first.

## Analysis Modal Over Separate Page
Clicking a PropCard opens a modal (not a new page) to keep context. Users can quickly compare multiple picks without navigation. Modal includes L5 chart, model context, AI Q&A, quantile distribution, line shopping, Kelly sizing.

## AI Q&A with Claude Haiku
Low cost (~$0.003/question) enables conversational analysis without budget concerns. 20 questions/day rate limit balances cost control with usability. 5 parallel data enrichment queries ground every answer in real data.

## DFS Edge Finder Independent of Model
Market edge (devigged consensus vs DFS lines) works for all 6 stats including STL, BLK, 3PM that the model doesn't predict. This doubles the DFS page's utility without requiring new models.

## Confidence Stars (1-5) on Take Bet
Visual confidence indicator helps users quickly assess conviction level. Based on the BL posterior probability and edge magnitude.

## Cross-Device Bet Tracking
`useUserBets` with optimistic UI + Supabase sync lets users take bets on phone and review on desktop. Essential for the mobile-first sports betting audience.

#product #decisions #ux
