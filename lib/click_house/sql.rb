# frozen_string_literal: true

require 'click_house/sql/insert'

module ClickHouse
  # Namespace containing SQL string builders
  module SQL
    module_function

    def quote(value)
      case value
      when Array then "[#{value.map { |item| quote(item) }.join(',')}]"
      when nil then 'NULL'
      when Date then "'#{value.strftime('%Y-%m-%d')}'"
      when DateTime, Time then "'#{value.strftime('%Y-%m-%d %H:%M:%S')}'"
      when Numeric then value
      when String then "'#{value.gsub(/['\\]/, '\\\\\0')}'"
      else fail ArgumentError, "Unexpected value type - #{value.class}"
      end
    end
  end
end
