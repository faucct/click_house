# frozen_string_literal: true

module ClickHouse
  module SQL
    # @see https://clickhouse.yandex/docs/en/query_language/select
    class Select < DSL
      CLAUSES_ORDER = %i[
        distinct
        expressions
        from
        where
        format
      ].freeze

      # @param clauses [{Symbol, String}]
      def initialize(**clauses)
        @clauses = clauses
      end

      # @return [String]
      def to_s
        ['SELECT', *@clauses.values_at(*CLAUSES_ORDER).compact].join(' ')
      end

      # @return [Select]
      # @see https://clickhouse.yandex/docs/en/query_language/select/#distinct-clause
      def distinct
        self.class.new(**@clauses, distinct: 'DISTINCT')
      end

      # @param expressions [<#to_s>]
      # @return [Select]
      # @see https://clickhouse.yandex/docs/en/query_language/select/#select-clause
      def expressions(*expressions)
        self.class.new(**@clauses, expressions: expressions.join(','))
      end

      # @param table [#to_s]
      # @param database [#to_s, nil]
      # @param final [true, false]
      # @return [Select]
      # @see https://clickhouse.yandex/docs/en/query_language/select/#from-clause
      def from(table, database: nil, final: false)
        self.class.new(**@clauses, from: "FROM #{[database, table].compact.join('.')}#{' FINAL' if final}")
      end

      # @param format [#to_s]
      # @return [Select]
      # @see https://clickhouse.yandex/docs/en/query_language/select/#format
      def format(format)
        self.class.new(**@clauses, format: "FORMAT #{format}")
      end

      # @param expression [String]
      # @return [Select]
      # https://clickhouse.yandex/docs/en/query_language/select/#where-clause
      def where(expression)
        self.class.new(**@clauses, where: expression)
      end
    end
  end
end
