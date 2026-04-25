'use client'

import Image from 'next/image'
import { useState } from 'react'
import { PLACEHOLDER_AVATAR } from '@/lib/utils'
import { useSport } from '@/contexts/SportContext'
import { getSportConfig, type Sport } from '@/lib/sport-config'

interface PlayerAvatarProps {
  playerId: number
  playerName: string
  size?: 'sm' | 'md' | 'lg'
  className?: string
  /** Override sport for headshot URL instead of reading global sport context */
  sportOverride?: Sport
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
  sportOverride,
}: PlayerAvatarProps) {
  const { config: globalConfig } = useSport()
  const config = sportOverride ? getSportConfig(sportOverride) : globalConfig
  const [hasError, setHasError] = useState(false)

  return (
    <div
      className={`relative rounded-full overflow-hidden bg-slate-700 ${sizeClasses[size]} ${className}`}
    >
      <Image
        src={hasError ? PLACEHOLDER_AVATAR : config.getHeadshotUrl(playerId)}
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
