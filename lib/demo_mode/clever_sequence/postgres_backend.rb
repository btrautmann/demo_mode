# frozen_string_literal: true

class CleverSequence
  module PostgresBackend
    SEQUENCE_PREFIX = 'cs_'

    class SequenceNotFoundError < StandardError
      attr_reader :sequence_name, :klass, :attribute

      def initialize(sequence_name:, klass:, attribute:)
        @sequence_name = sequence_name
        @klass = klass
        @attribute = attribute

        super(
          "Sequence '#{sequence_name}' not found for #{klass.name}##{attribute}. "
        )
      end
    end

    module SequenceResult
      Exists = Data.define(:sequence_name)
      Missing = Data.define(:sequence_name, :klass, :attribute, :calculated_start_value)
    end

    class << self
      def with_sequence_adjustment
        Thread.current[:clever_sequence_adjust_sequences_enabled] = true
        yield
      ensure
        Thread.current[:clever_sequence_adjust_sequences_enabled] = false
      end

      def nextval(klass, attribute, block)
        name = sequence_name(klass, attribute)

        if sequence_exists?(name)
          # On first use with adjustment enabled, ensure sequence is past existing data
          if adjust_sequences_enabled? && !sequence_cache[name].is_a?(SequenceResult::Exists)
            adjust_sequence_if_needed(name, klass, attribute, block)
          end
          sequence_cache[name] = SequenceResult::Exists.new(name)

          result = ActiveRecord::Base.connection.execute(
            "SELECT nextval('#{name}')",
          )
          result.first['nextval'].to_i
        else
          # Check if we already have this sequence cached as Missing
          cached = sequence_cache[name]

          if cached.is_a?(SequenceResult::Missing)
            # Increment from cached value instead of recalculating from DB
            # This handles the case where transactions are rolled back but we
            # need to continue generating unique values
            next_value = cached.calculated_start_value + 1
            sequence_cache[name] = SequenceResult::Missing.new(
              sequence_name: name,
              klass: klass,
              attribute: attribute,
              calculated_start_value: next_value,
            )
          else
            # First time seeing this missing sequence - calculate from DB
            start_value = calculate_sequence_value(klass, attribute, block)
            next_value = start_value + 1
            sequence_cache[name] = SequenceResult::Missing.new(
              sequence_name: name,
              klass: klass,
              attribute: attribute,
              calculated_start_value: next_value,
            )
          end

          if CleverSequence.enforce_sequences_exist
            raise SequenceNotFoundError.new(
              sequence_name: name,
              klass: klass,
              attribute: attribute,
            )
          else
            next_value
          end
        end
      end

      def sequence_name(klass, attribute)
        table = klass.table_name.gsub(/[^a-z0-9_]/i, '_')
        attr = attribute.to_s.gsub(/[^a-z0-9_]/i, '_')
        # Handle PostgreSQL identifier limit:
        limit = (63 - SEQUENCE_PREFIX.length) / 2
        # Lowercase to avoid PostgreSQL case-sensitivity issues with unquoted identifiers
        "#{SEQUENCE_PREFIX}#{table[0, limit]}_#{attr[0, limit]}".downcase
      end

      def sequence_cache
        @sequence_cache ||= {}
      end

      def clear_sequence_cache!
        # Preserve Missing entries since those are needed for sequence discovery
        # Only clear Exists entries so sequences get re-checked and potentially adjusted
        @sequence_cache = sequence_cache.select { |_, v| v.is_a?(SequenceResult::Missing) }
      end

      private

      def adjust_sequences_enabled?
        Thread.current[:clever_sequence_adjust_sequences_enabled]
      end

      def sequence_exists?(sequence_name)
        if sequence_cache.key?(sequence_name)
          case sequence_cache[sequence_name]
          when SequenceResult::Exists
            return true
          else
            return false
          end
        end

        ActiveRecord::Base.connection.execute(
          "SELECT 1 FROM information_schema.sequences WHERE sequence_name = '#{sequence_name}' LIMIT 1",
        ).any?
      end

      def calculate_sequence_value(klass, attribute, block)
        column_name = klass.attribute_aliases.fetch(attribute.to_s, attribute.to_s)
        return 0 unless klass.column_names.include?(column_name)

        ActiveRecord::Base.with_transactional_lock("lower-bound-#{klass}-#{column_name}") do
          LowerBoundFinder.new(klass, column_name, block).lower_bound
        end
      end

      def adjust_sequence_if_needed(sequence_name, klass, attribute, block)
        max_value = calculate_sequence_value(klass, attribute, block)
        return if max_value < 1

        # setval sets the sequence's last_value. With the default 3rd argument (true),
        # the next nextval() will return last_value + 1.
        # We only want to advance (never go backwards), so we use GREATEST.
        ActiveRecord::Base.connection.execute(<<~SQL.squish)
          SELECT setval('#{sequence_name}', GREATEST(#{max_value}, (SELECT last_value FROM #{sequence_name})))
        SQL
      end
    end
  end
end
