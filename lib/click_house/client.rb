# frozen_string_literal: true

require 'click_house/client/select_output_parser'

module ClickHouse
  # Performs queries
  class Client
    # @param http_interface [HTTPInterface]
    def initialize(http_interface: HTTPInterface.new)
      @http_interface = http_interface
    end

    def select_from_sql(sql, format:, **parser_options)
      selected = nil
      @http_interface.get(query: sql) do |response_io|
        selected = SelectOutputParser.public_send(
          format,
          Enumerator.new { |y| response_io.read_body { |chunk| y << chunk } },
          **parser_options,
        ).to_a
      end
      selected
    end
  end
end
