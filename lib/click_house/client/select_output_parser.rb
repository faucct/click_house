# frozen_string_literal: true

module ClickHouse
  class Client
    # @see https://clickhouse.yandex/docs/en/interfaces/formats/
    module SelectOutputParser
      STRING_SUBSTITUTIONS = {
        '\\b' => "\b",
        '\\f' => "\f",
        '\\r' => "\r",
        '\\n' => "\n",
        '\\t' => "\t",
        '\\0' => "\0",
        "\\'" => "'",
        '\\\\' => '\\'
      }.freeze
      # @see https://clickhouse.yandex/docs/en/interfaces/formats/#tabseparated
      TYPED_PARSER_BUILDER = lambda do |type|
        case type
        when /^Array\((.+)\)$/
          item_builder = TYPED_PARSER_BUILDER.call(Regexp.last_match[1])
          ->(value) { value.match(/^\[(.*)\]$/)[1].split(',').map(&item_builder) }
        when /^Nullable\((.+)\)$/
          not_null_builder = TYPED_PARSER_BUILDER.call(Regexp.last_match[1])
          ->(value) { not_null_builder.call(value) unless value == '\N' }
        when /^U?Int(?:8|16|32|64)$/ then :to_i
        when /^Float(?:32|64)$/ then :to_f
        when /^Enum(?:8|16)\(.+\)$/, /^FixedString\(\d+\)$/, 'String'
          lambda do |escaped|
            escaped
              .gsub(/#{STRING_SUBSTITUTIONS.each_key.map(&Regexp.method(:escape)).join('|')}/, STRING_SUBSTITUTIONS)
              .gsub(/\A'(.*)'\z/, '\1')
              .tap { |unescaped| unescaped.force_encoding Encoding.default_internal if Encoding.default_internal }
          end
        when 'Date' then ->(value) { Date.strptime(value, '%Y-%m-%d') }
        when 'DateTime' then ->(value) { Time.strptime(value, '%Y-%m-%d %H:%M:%S') }
        when 'Nothing' then ->(_) {}
        else fail NotImplementedError, "Unknown type #{type.inspect}"
        end.to_proc
      end

      def self.tsv_with_names_and_types(body, **options)
        fail ArgumentError unless options.empty?
        Enumerator.new do |y|
          rows = TSV.parse(body)
          parsers = rows.next.map(&TYPED_PARSER_BUILDER)
          loop { y << parsers.zip(rows.next.to_a).map { |parser, value| parser.call(value) } }
        end
      end
    end
  end
end
