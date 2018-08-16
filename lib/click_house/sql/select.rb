module ClickHouse
  # https://clickhouse.yandex/docs/en/query_language/select
  module SQL
    SELECT_CLAUSE_BUILDERS = {
      # https://clickhouse.yandex/docs/en/query_language/select/#distinct-clause
      distinct: ->(distinct) { 'DISTINCT' if distinct },
      # https://clickhouse.yandex/docs/en/query_language/select/#select-clause
      expressions: ->(*expressions) { expressions.join(',') },
      # https://clickhouse.yandex/docs/en/query_language/select/#from-clause
      from: lambda do |database = nil, table, final: false|
        "FROM #{[database, table].compact.join('.')}#{' FINAL' if final}"
      end,
      # https://clickhouse.yandex/docs/en/query_language/select/#format
      format: ->(format) { "FORMAT #{format}" }
    }.freeze

    # @return [String]
    def self.select(**clauses)
      ['SELECT', *SELECT_CLAUSE_BUILDERS.map do |key, builder|
        builder.call(*clauses[key]) if clauses.key?(key)
      end.compact].join(' ')
    end
  end
end
