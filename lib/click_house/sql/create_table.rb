module ClickHouse
  # https://clickhouse.yandex/docs/en/query_language/create/#create-table
  module SQL
    # https://clickhouse.yandex/docs/en/query_language/create/#default-values
    CREATE_TABLE_COLUMN_CLAUSE_BUILDERS = {
      default: ->(default) { "DEFAULT #{default}" },
      materialized: ->(materialized) { "MATERIALIZED #{materialized}" },
      alias: ->(alias_) { "ALIAS #{alias_}" }
    }.freeze
    CREATE_TABLE_CLAUSE_BUILDERS = {
      # https://clickhouse.yandex/docs/en/query_language/create/#temporary-tables
      temporary: ->(temporary) { 'TEMPORARY' if temporary },
      table: -> { 'TABLE' },
      if_not_exists: ->(if_not_exists) { 'IF NOT EXISTS' if if_not_exists },
      name: ->(db = nil, name) { [db, name].compact.join('.') },
      # https://clickhouse.yandex/docs/en/query_language/create/#distributed-ddl-queries-on-cluster-clause
      on_cluster: ->(cluster) { "ON CLUSTER #{cluster}" if cluster },
      columns: lambda do |*columns_with_types|
        "(#{columns_with_types.map do |column, type, **clauses|
          [column, type, *CREATE_TABLE_COLUMN_CLAUSE_BUILDERS.map do |key, builder|
            builder.call(*clauses[key]) if clauses.key?(key)
          end.compact].join(' ')
        end.join(',')})"
      end,
      engine: ->(engine) { "ENGINE = #{engine}" }
    }.freeze

    # @return [String]
    def self.create_table(**clauses)
      ['CREATE TABLE', *CREATE_TABLE_CLAUSE_BUILDERS.map do |key, builder|
        builder.call(*clauses[key]) if clauses.key?(key)
      end.compact].join(' ')
    end
  end
end
