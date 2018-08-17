# frozen_string_literal: true

require 'click_house/client/select_output_parser'

module ClickHouse
  # Performs queries
  class Client
    # @param http_interface [HTTPInterface]
    def initialize(http_interface: HTTPInterface.new)
      @http_interface = http_interface
    end

    # @return [Enumerable<Array>]
    def each_selected_row
      each_selected_row_from_sql(
        format: :tab_separated_with_names_and_types,
        sql: SQL::Select.call { |select| yield(select).format(:TabSeparatedWithNamesAndTypes) },
      )
    end

    # @param sql [String]
    # @param format [Symbol] public methods of SelectOutputParser
    # @return [Enumerable<Array>]
    def each_selected_row_from_sql(sql:, format:, **parser_options)
      SelectOutputParser.public_send(format, @http_interface.get(query: sql), **parser_options)
    end

    # @return [<Array>]
    def select_rows(&block)
      each_selected_row(&block).to_a
    end

    # @param rows [<<*>>]
    def insert(rows:, **options)
      @http_interface.post(query: SQL::Insert.call(options), body: SQL::Insert.values(rows))
    end

    # https://clickhouse.yandex/docs/en/query_language/create/#create-database
    def create_database(db_name, if_not_exists: false)
      @http_interface.post(query: ['CREATE DATABASE', ('IF NOT EXISTS' if if_not_exists), db_name].compact.join(' '))
    end

    def create_table(&block)
      @http_interface.post(query: SQL::CreateTable.call(&block))
    end
  end
end
