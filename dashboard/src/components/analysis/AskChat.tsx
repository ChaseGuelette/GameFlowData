'use client'

import { useState, useRef, useEffect, useCallback } from 'react'
import ReactMarkdown from 'react-markdown'
import { type ChatMessage, type AskResponse, type ChatHistoryResponse } from '@/types/chat'
import { type Prediction, type PlayerGameStats, type BookmakerLine } from '@/types/predictions'
import { type Insight } from '@/lib/insights'

interface AskChatProps {
  prediction: Prediction
  history: PlayerGameStats[]
  insights: Insight[]
  bookmakerLines: BookmakerLine[]
  isOverBet: boolean
  edge: number
  probability: number
}

const SUGGESTED_QUESTIONS = [
  'Why is the model bullish on this?',
  'How has workload affected output?',
  'What does the opponent matchup say?',
]

export function AskChat({
  prediction,
  history,
  insights,
  bookmakerLines,
  isOverBet,
  edge,
  probability,
}: AskChatProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [isOpen, setIsOpen] = useState(false)
  const [remaining, setRemaining] = useState<number | null>(null)
  const [conversationId, setConversationId] = useState<number | null>(null)
  const [historyLoaded, setHistoryLoaded] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const abortRef = useRef<AbortController | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  // Scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Cancel in-flight request on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort()
    }
  }, [])

  // Load conversation history on mount
  useEffect(() => {
    if (historyLoaded) return
    const gameDate = prediction.prediction_date || new Date().toISOString().split('T')[0]

    fetch(`/api/ask/history?player_id=${prediction.player_id}&stat=${prediction.stat}&game_date=${gameDate}`)
      .then(res => res.ok ? res.json() : null)
      .then((data: ChatHistoryResponse | null) => {
        if (data && data.messages.length > 0) {
          setMessages(data.messages)
          setConversationId(data.conversation_id)
        }
        setHistoryLoaded(true)
      })
      .catch(() => setHistoryLoaded(true))
  }, [prediction.player_id, prediction.stat, prediction.prediction_date, historyLoaded])

  const clearConversation = useCallback(async () => {
    if (!conversationId) {
      setMessages([])
      return
    }

    try {
      await fetch(`/api/ask/history?conversation_id=${conversationId}`, { method: 'DELETE' })
    } catch {
      // Ignore errors — clear locally regardless
    }
    setMessages([])
    setConversationId(null)
  }, [conversationId])

  const sendQuestion = useCallback(async (question: string) => {
    if (!question.trim() || loading) return

    const userMessage: ChatMessage = { role: 'user', content: question.trim() }
    const newMessages = [...messages, userMessage].slice(-5)
    setMessages(prev => [...prev, userMessage])
    setInput('')
    setLoading(true)

    abortRef.current?.abort()
    const controller = new AbortController()
    abortRef.current = controller

    try {
      const res = await fetch('/api/ask', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question: question.trim(),
          conversationHistory: newMessages.slice(0, -1), // exclude the new user message
          playerContext: {
            prediction,
            history,
            insights,
            bookmakerLines,
            isOverBet,
            edge,
            probability,
          },
        }),
        signal: controller.signal,
      })

      const data: AskResponse = await res.json()

      if (!res.ok) {
        setMessages(prev => [
          ...prev,
          { role: 'assistant', content: data.error || 'Something went wrong.' },
        ])
      } else {
        setMessages(prev => [
          ...prev,
          { role: 'assistant', content: data.answer },
        ])
        setRemaining(data.remaining)
        if (data.conversation_id) {
          setConversationId(data.conversation_id)
        }
      }
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: 'Failed to get response. Please try again.' },
      ])
    } finally {
      setLoading(false)
    }
  }, [messages, loading, prediction, history, insights, bookmakerLines, isOverBet, edge, probability])

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendQuestion(input)
    }
    // Prevent Escape from bubbling up and closing the modal
    if (e.key === 'Escape') {
      e.stopPropagation()
      inputRef.current?.blur()
    }
  }

  return (
    <div className="border-b border-slate-700">
      {/* Collapsible header */}
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between p-4 sm:p-6 hover:bg-slate-700/30 transition-colors"
      >
        <h3 className="text-lg font-semibold text-slate-50 flex items-center gap-2">
          <span className="text-base">AI</span>
          Ask AI about this pick
          {messages.length > 0 && !isOpen && (
            <span className="text-xs font-normal text-slate-400 ml-1">
              ({messages.length} messages)
            </span>
          )}
        </h3>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          viewBox="0 0 20 20"
          fill="currentColor"
          className={`w-5 h-5 text-slate-400 transition-transform ${isOpen ? 'rotate-180' : ''}`}
        >
          <path fillRule="evenodd" d="M5.22 8.22a.75.75 0 0 1 1.06 0L10 11.94l3.72-3.72a.75.75 0 1 1 1.06 1.06l-4.25 4.25a.75.75 0 0 1-1.06 0L5.22 9.28a.75.75 0 0 1 0-1.06Z" clipRule="evenodd" />
        </svg>
      </button>

      {isOpen && (
        <div className="px-4 sm:px-6 pb-4 sm:pb-6">
          {/* Suggested questions (shown when no messages yet) */}
          {messages.length === 0 && (
            <div className="flex flex-wrap gap-2 mb-3">
              {SUGGESTED_QUESTIONS.map((q, i) => (
                <button
                  key={i}
                  type="button"
                  onClick={() => sendQuestion(q)}
                  disabled={loading}
                  className="text-xs bg-slate-700/60 hover:bg-slate-600/60 text-slate-300 px-3 py-1.5 rounded-full transition-colors disabled:opacity-50"
                >
                  {q}
                </button>
              ))}
            </div>
          )}

          {/* Messages area */}
          {messages.length > 0 && (
            <div className="max-h-80 overflow-y-auto space-y-3 mb-3 scrollbar-thin">
              {messages.map((msg, i) => (
                <div
                  key={i}
                  className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                >
                  <div
                    className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
                      msg.role === 'user'
                        ? 'bg-blue-600/30 text-blue-100 whitespace-pre-wrap'
                        : 'bg-slate-700/60 text-slate-200'
                    }`}
                  >
                    {msg.role === 'assistant' ? (
                      <div className="prose prose-sm prose-invert max-w-none [&>p]:my-1 [&>ul]:my-1 [&>ol]:my-1">
                        <ReactMarkdown>{msg.content}</ReactMarkdown>
                      </div>
                    ) : (
                      msg.content
                    )}
                  </div>
                </div>
              ))}
              {loading && (
                <div className="flex justify-start">
                  <div className="bg-slate-700/60 rounded-lg px-3 py-2 text-sm text-slate-400">
                    <span className="inline-flex gap-1">
                      <span className="animate-bounce" style={{ animationDelay: '0ms' }}>.</span>
                      <span className="animate-bounce" style={{ animationDelay: '150ms' }}>.</span>
                      <span className="animate-bounce" style={{ animationDelay: '300ms' }}>.</span>
                    </span>
                  </div>
                </div>
              )}
              <div ref={messagesEndRef} />
            </div>
          )}

          {/* Input row */}
          <div className="flex items-center gap-2">
            <input
              ref={inputRef}
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value.slice(0, 500))}
              onKeyDown={handleKeyDown}
              placeholder="Ask a question..."
              disabled={loading}
              className="flex-1 bg-slate-700 border border-slate-600 rounded px-3 py-2 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500 disabled:opacity-50"
            />
            <button
              type="button"
              onClick={() => sendQuestion(input)}
              disabled={loading || !input.trim()}
              className="bg-blue-600 hover:bg-blue-500 disabled:bg-slate-700 disabled:text-slate-500 text-white text-sm px-3 py-2 rounded font-medium transition-colors"
            >
              Send
            </button>
          </div>

          {/* Footer: char count + remaining + clear */}
          <div className="flex items-center justify-between mt-1.5 text-xs text-slate-500">
            <span>{input.length > 0 ? `${input.length}/500` : ''}</span>
            <div className="flex items-center gap-3">
              {messages.length > 0 && (
                <button
                  type="button"
                  onClick={clearConversation}
                  className="text-slate-500 hover:text-slate-300 transition-colors"
                >
                  Clear chat
                </button>
              )}
              {remaining !== null && (
                <span>{remaining} question{remaining !== 1 ? 's' : ''} left today</span>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
