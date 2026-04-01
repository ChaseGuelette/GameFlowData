export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
}

export interface AskResponse {
  answer: string
  remaining: number
  conversation_id?: number
  error?: string
}

export interface ChatHistoryResponse {
  conversation_id: number | null
  messages: ChatMessage[]
}
