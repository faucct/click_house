# frozen_string_literal: true

module ClickHouse
  module SQL
    # Defines a common interface used in `SQLBuilder`.
    class DSL
      # @return [String]
      def self.call(*args)
        if block_given?
          yield(new(*args)).to_s
        else
          new(*args)
        end
      end

      # @return [String]
      def to_s
        fail NotImplementedError
      end
    end
  end
end
