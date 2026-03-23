export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
}

export interface AskResponse {
  answer: string
  remaining: number
  error?: string
}
