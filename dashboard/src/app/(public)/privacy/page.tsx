export default function PrivacyPage() {
  return (
    <div className="py-16 sm:py-24">
      <div className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
        <h1 className="text-3xl font-bold text-slate-50 mb-8">Privacy Policy</h1>
        <div className="prose prose-invert prose-slate max-w-none space-y-6 text-slate-300 text-sm leading-relaxed">
          <p><strong>Effective Date:</strong> February 18, 2026</p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">1. Information We Collect</h2>
          <p>
            We collect information you provide when creating an account, including your email address.
            We also collect usage data such as pages visited and features used to improve the Service.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">2. How We Use Your Information</h2>
          <p>We use your information to:</p>
          <ul className="list-disc pl-6 space-y-1">
            <li>Provide and maintain the Service</li>
            <li>Manage your account and access</li>
            <li>Send account-related communications</li>
            <li>Improve our predictions and user experience</li>
          </ul>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">3. Payment Processing</h2>
          <p>
            The Service is currently free during beta. We may add payment processing in the future,
            at which point this section will be updated with details about how payment data is handled.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">4. Data Sharing</h2>
          <p>
            We do not sell your personal information. We may share data with third-party service
            providers (e.g., Stripe for payments, Supabase for authentication) as necessary to
            operate the Service.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">5. Data Security</h2>
          <p>
            We implement industry-standard security measures including encrypted connections (HTTPS),
            row-level security on our database, and secure authentication via Supabase Auth.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">6. Cookies</h2>
          <p>
            We use essential cookies for authentication and session management. We do not use
            third-party tracking cookies.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">7. Your Rights</h2>
          <p>
            You may request access to, correction of, or deletion of your personal data by
            contacting us. You may delete your account at any time from your account settings.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">8. Changes to This Policy</h2>
          <p>
            We may update this privacy policy from time to time. We will notify you of material
            changes via email or through the Service.
          </p>

          <h2 className="text-xl font-semibold text-slate-100 mt-8">9. Contact</h2>
          <p>
            For privacy-related questions, contact us at support@gameflowdata.com.
          </p>
        </div>
      </div>
    </div>
  )
}
