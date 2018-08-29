# frozen_string_literal: true

require 'click_house/sql/dsl'
require 'click_house/sql/create_table'
require 'click_house/sql/insert'
require 'click_house/sql/select'

module ClickHouse
  # Namespace containing SQL string builders
  module SQL
    def self.quote(value)
      case value
      when Array then "[#{value.map { |item| quote(item) }.join(',')}]"
      when nil then 'NULL'
      when Date then "'#{value.strftime('%Y-%m-%d')}'"
      when DateTime, Time then "'#{value.utc.strftime('%Y-%m-%d %H:%M:%S')}'"
      when Numeric then value.to_s
      when String then "'#{value.to_s.gsub(/['\\]/, '\\\\\0')}'"
      else fail ArgumentError, "Unexpected value type - #{value.class}"
      end
    end

    def self.quote_identifier(identifier)
      identifier = identifier.to_s
      fail ArgumentError unless identifier.match?(/^[a-zA-Z_][0-9a-zA-Z_]*$/)
      "`#{identifier}`"
    end
  end
end
