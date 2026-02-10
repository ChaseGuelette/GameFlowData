'use client'

import Image from 'next/image'
import { useState } from 'react'
import { getHeadshotUrl, PLACEHOLDER_AVATAR } from '@/lib/utils'

interface PlayerAvatarProps {
  playerId: number
  playerName: string
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

const sizeClasses = {
  sm: 'w-10 h-10',
  md: 'w-16 h-16',
  lg: 'w-24 h-24',
}

const imageSizes = {
  sm: 40,
  md: 64,
  lg: 96,
}

export function PlayerAvatar({
  playerId,
  playerName,
  size = 'md',
  className = '',
}: PlayerAvatarProps) {
  const [hasError, setHasError] = useState(false)

  return (
    <div
      className={`relative rounded-full overflow-hidden bg-slate-700 ${sizeClasses[size]} ${className}`}
    >
      <Image
        src={hasError ? PLACEHOLDER_AVATAR : getHeadshotUrl(playerId)}
        alt={playerName}
        width={imageSizes[size]}
        height={imageSizes[size]}
        className="object-cover object-top"
        onError={() => setHasError(true)}
        unoptimized
      />
    </div>
  )
}
