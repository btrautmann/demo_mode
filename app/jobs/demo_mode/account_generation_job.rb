# frozen_string_literal: true

module DemoMode
  class AccountGenerationJob < DemoMode.base_job_name.constantize
    def perform(session, **options)
      ActiveSupport::Notifications.instrument(
        'demo_mode.account_generation',
        persona_name: session.persona_name,
        variant: session.variant,
      ) do
        session.with_lock do
          persona = session.persona
          raise "Unknown persona: #{session.persona_name}" if persona.blank?

          signinable = persona.generate!(variant: session.variant, password: session.signinable_password, options: options)
          session.update!(signinable: signinable, status: 'successful')
        end
      end
    rescue StandardError => e
      session.update!(status: 'failed')
      raise e
    end
  end
end
