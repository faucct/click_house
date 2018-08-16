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
      SelectOutputParser.public_send(format, @http_interface.get(query: sql), **parser_options).to_a
    end
  end
end
