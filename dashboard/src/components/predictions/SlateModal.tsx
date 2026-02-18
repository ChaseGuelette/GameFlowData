'use client'

import { useEffect } from 'react'

interface SlateModalProps {
  imageUrl: string
  onClose: () => void
}

export function SlateModal({ imageUrl, onClose }: SlateModalProps) {
  // Handle escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [onClose])

  const handleDownload = () => {
    const link = document.createElement('a')
    link.href = imageUrl
    link.download = `gameflow-slate-${new Date().toISOString().split('T')[0]}.png`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/70" onClick={onClose} />

      {/* Modal */}
      <div className="relative bg-slate-800 rounded-lg border border-slate-700 w-full max-w-lg max-h-[90vh] overflow-y-auto">
        {/* Close button */}
        <button
          onClick={onClose}
          className="absolute top-4 right-4 text-slate-400 hover:text-slate-200 text-2xl z-10"
        >
          &times;
        </button>

        {/* Header */}
        <div className="p-6 pb-3">
          <h2 className="text-xl font-bold text-slate-50">Your Slate</h2>
          <p className="text-slate-400 text-sm mt-1">Save and share on social media!</p>
        </div>

        {/* Image preview */}
        <div className="px-6">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src={imageUrl}
            alt="Slate picks"
            className="w-full rounded-lg border border-slate-700"
          />
        </div>

        {/* Actions */}
        <div className="p-6 flex gap-3">
          <button
            onClick={handleDownload}
            className="flex-1 py-3 px-4 bg-blue-600 hover:bg-blue-500 text-white rounded-md font-medium transition-colors"
          >
            Download PNG
          </button>
          <button
            onClick={onClose}
            className="py-3 px-4 bg-slate-700 hover:bg-slate-600 text-slate-200 rounded-md font-medium transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  )
}
