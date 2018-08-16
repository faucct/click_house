module ClickHouse
  # Performs queries
  class Client
    # @param http_interface [HTTPInterface]
    def initialize(http_interface: HTTPInterface.new)
      @http_interface = http_interface
    end

    # https://clickhouse.yandex/docs/en/interfaces/formats/#tabseparated
    TYPED_PARSER_BUILDER = lambda do |type|
      case type
      when /^Array\((.+)\)$/
        item_builder = TYPED_PARSER_BUILDER.call(Regexp.last_match[1])
        ->(value) { value.match(/^\[(.*)\]$/)[1].split(',').map(&item_builder) }
      when /^Nullable\((.+)\)$/
        not_null_builder = TYPED_PARSER_BUILDER.call(Regexp.last_match[1])
        ->(value) { not_null_builder.call(value) unless value == '\N' }
      when /^U?Int(?:8|16|32|64)$/
        :to_i
      when /^Float(?:32|64)$/
        :to_f
      when /^Enum(?:8|16)\(.+\)$/, /^FixedString\(\d+\)$/, 'String'
        ->(value) { value.gsub(/\\([\b\f\r\n\t\0'\\])/, '\1').gsub(/\A'(.*)'\z/, '\1') }
      when 'Date'
        ->(value) { Date.strptime(value, '%Y-%m-%d') }
      when 'DateTime'
        ->(value) { Time.strptime(value, '%Y-%m-%d %H:%M:%S') }
      else
        fail NotImplementedError, "Unknown type #{type.inspect}"
      end.to_proc
    end

    # @return [Enumerable<Array>]
    def each_selected_row(**clauses)
      Enumerator.new do |y|
        TSV.parse(@http_interface.get(query: SQL.select(**clauses, format: 'TabSeparatedWithNamesAndTypes')))
           .inject(nil) do |types, row|
          next row.map(&TYPED_PARSER_BUILDER) unless types
          y << types.zip(row.to_a).map { |type, value| type.call(value) }
          types
        end
      end
    end

    # @return [<Array>]
    def select_rows(**clauses)
      each_selected_row(clauses).to_a
    end
  end
end
