module ClickHouse
  # https://clickhouse.yandex/docs/en/query_language/insert_into/
  module SQL
    INSERT_VALUE_STRINGIFIER = lambda do |value|
      case value
      when Array then "[#{value.map(&INSERT_VALUE_STRINGIFIER).join(',')}]"
      when nil then 'NULL'
      when Date then "'#{value.strftime('%Y-%m-%d')}'"
      when DateTime, Time then "'#{value.strftime('%Y-%m-%d %H:%M:%S')}'"
      when Numeric then value.to_s
      when String then "'#{value.to_s.gsub(/['\\]/, '\\\\\0')}'"
      else fail ArgumentError, "Unexpected value type - #{value.class}"
      end
    end

    # @param into [#to_s, (#to_s, #to_s)]
    # @param columns [<#to_s>, nil]
    # @param rows [<<*>>, nil]
    # @return [String]
    def self.insert(into:, columns: nil, rows: nil)
      [
        "INSERT INTO #{Array(into).join('.')}",
        ("(#{columns.map { |column| Array(column).join('.') }.join(',')})" if columns),
        (insert_values(rows) if rows),
      ].compact.join(' ')
    end

    # @param rows [<<*>>]
    # @return [String]
    def self.insert_values(rows)
      "VALUES #{rows.map { |row| "(#{row.map(&INSERT_VALUE_STRINGIFIER).join(',')})" }.join(',')}"
    end
  end
end
