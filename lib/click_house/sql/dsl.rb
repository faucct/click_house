# frozen_string_literal: true

module ClickHouse
  module SQL
    # Defines a common interface used in `SQLBuilder`.
    class DSL
      # @return [String]
      def self.call(*args)
        yield(new(*args)).to_s
      end

      # @return [String]
      def to_s
        fail NotImplementedError
      end
    end
  end
end
