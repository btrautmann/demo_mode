# frozen_string_literal: true

module DemoMode
  class AccountGenerationJob < DemoMode.base_job_name.constantize
    attr_reader :session

    # Called by Delayed::Plugins::LoggingPlugin to log job arguments.
    # Returns a hash of arguments to be logged as structured data.
    # Uses `arguments` (available on ActiveJob instances) rather than instance
    # variables, since the LoggingPlugin deserializes the job after execution.
    def loggable_arguments
      session = arguments.first
      return {} unless session

      {
        session_id: session.id,
        persona_name: session.persona_name,
        variant: session.variant&.to_s,
      }
    end

    # Opt-in to flatten nested arguments for easier Datadog faceting.
    def flatten_loggable_arguments?
      true
    end

    def perform(session, **options)
      @session = session
      session.with_lock do
        persona = session.persona
        raise "Unknown persona: #{session.persona_name}" if persona.blank?

        signinable = persona.generate!(variant: session.variant, password: session.signinable_password, options: options)
        session.update!(signinable: signinable, status: 'successful')
      end
    rescue StandardError => e
      session.update!(status: 'failed')
      raise e
    end
  end
end
