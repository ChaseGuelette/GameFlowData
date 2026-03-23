import { HeroSection } from '@/components/landing/HeroSection'
import { FeatureGrid } from '@/components/landing/FeatureGrid'
import { ProductScreenshots } from '@/components/landing/ProductScreenshots'
import { SupportedMarkets } from '@/components/landing/SupportedMarkets'
import { WinningBetSlips } from '@/components/landing/WinningBetSlips'
import { Testimonials } from '@/components/landing/Testimonials'
import { CtaSection } from '@/components/landing/CtaSection'

export default function LandingPage() {
  return (
    <>
      <HeroSection />
      <FeatureGrid />
      <ProductScreenshots />
      <SupportedMarkets />
      <WinningBetSlips />
      <Testimonials />
      <CtaSection />
    </>
  )
}
