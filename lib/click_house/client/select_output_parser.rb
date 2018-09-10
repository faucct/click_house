# frozen_string_literal: true

module ClickHouse
  class Client
    # @see https://clickhouse.yandex/docs/en/interfaces/formats/
    module SelectOutputParser
      def self.tsv_with_names_and_types(chunks_enum, **options)
        fail ArgumentError unless options.empty?

        Enumerator.new do |y|
          rows = TSV.rows_enum(chunks_enum).tap(&:next)
          parsers = rows.next.map { |value| TSV.typed_pattern_parser(value) }
          loop { y << parsers.zip(rows.next).map { |parser, value| parser.parse(value) } }
        end
      end
    end
  end
end
