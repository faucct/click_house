# frozen_string_literal: true

module ClickHouse
  module SQL
    # https://clickhouse.yandex/docs/en/query_language/create/#create-table
    class CreateTable < DSL
      # @usage
      #   Columns.call { |column| column.(:id, :UInt32).(:foo, :String) }
      #   # => 'id UInt32, foo String'
      class Columns < DSL
        def initialize(columns = [])
          @columns = columns
        end

        def to_s
          @columns.map { |*clauses| clauses.compact.join(' ') }.join(',')
        end

        # @param name [#to_s]
        # @param type [#to_s]
        # @return [Columns]
        def call(name, type, &block)
          self.class.new([*@columns, [name, type, block&.call(Column.new)]])
        end
      end

      # https://clickhouse.yandex/docs/en/query_language/create/#default-values
      class Column
        CLAUSES_ORDER = %i[
          default
          materialized
          alias
        ].freeze

        def initialize(**clauses)
          @clauses = clauses
        end

        def to_s
          @clauses.values_at(*CLAUSES_ORDER).compact.join(' ')
        end

        # @return [Column]
        def default(expression)
          self.class.new(**@clauses, default: "DEFAULT #{expression}")
        end

        # @return [Column]
        def materialized(expression)
          self.class.new(**@clauses, materialized: "MATERIALIZED #{expression}")
        end

        # @return [Column]
        def alias(expression)
          self.class.new(**@clauses, alias: "ALIAS #{expression}")
        end
      end

      # https://clickhouse.yandex/docs/en/operations/table_engines/
      class Engine < DSL
        def initialize(clause = nil)
          @clause = clause
        end

        # @return [String]
        def to_s
          @clause || fail('No engine selected')
        end

        def distributed(cluster, remote_database, remote_table, sharding_key: nil)
          self.class.new("Distributed(#{cluster}, #{remote_database}, #{remote_table}#{", #{sharding_key}" if sharding_key})")
        end

        def merge_tree(date_column, primary_key, index_granularity)
          self.class.new("MergeTree(#{date_column}, (#{Array(primary_key).join(',')}), #{index_granularity})")
        end

        # https://clickhouse.yandex/docs/en/operations/table_engines/replacingmergetree/
        def replacing_merge_tree(order_by:)
          self.class.new("ReplacingMergeTree ORDER BY (#{Array(order_by).join(',')})")
        end

        def tiny_log
          self.class.new('TinyLog')
        end
      end

      AFTER_TABLE_CLAUSES_ORDER = %i[
        if_not_exists
        name
        on_cluster
        columns
        engine
      ].freeze

      def initialize(**clauses)
        @clauses = clauses
      end

      # @return [String]
      def to_s
        [
          'CREATE',
          *@clauses.values_at(:temporary).compact,
          'TABLE',
          *@clauses.values_at(*AFTER_TABLE_CLAUSES_ORDER).compact,
        ].join(' ')
      end

      # @return [CreateTable]
      # @see https://clickhouse.yandex/docs/en/query_language/create/#temporary-tables
      def temporary
        self.class.new(**@clauses, temporary: 'TEMPORARY')
      end

      # @return [CreateTable]
      def if_not_exists
        self.class.new(**@clauses, if_not_exists: 'IF NOT EXISTS')
      end

      # @param name [#to_s]
      # @param database [#to_s, nil]
      # @return [CreateTable]
      def name(name, database: nil)
        self.class.new(**@clauses, name: [database, name].compact.join('.'))
      end

      # @param cluster [#to_s]
      # @see https://clickhouse.yandex/docs/en/query_language/create/#distributed-ddl-queries-on-cluster-clause
      def on_cluster(cluster)
        self.class.new(**@clauses, on_cluster: "ON CLUSTER #{cluster}")
      end

      # @return [CreateTable]
      def columns(&block)
        self.class.new(**@clauses, columns: "(#{Columns.call(&block)})")
      end

      # @return [CreateTable]
      def engine(&block)
        self.class.new(**@clauses, engine: "ENGINE = #{Engine.call(&block)}")
      end
    end
  end
end
