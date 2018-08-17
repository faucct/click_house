# frozen_string_literal: true

module ClickHouse
  # https://clickhouse.yandex/docs/en/interfaces/http_interface/
  class HTTPInterface
    def initialize(**options)
      if options.key?(:url)
        initialize_from_url options
      else
        initialize_from_uri_components options
      end
    end

    def get(query: nil, body: nil)
      request(Net::HTTP::Get, query, body)
    end

    def post(query: nil, body: nil)
      request(Net::HTTP::Post, query, body)
    end

    private

    def initialize_from_url(url:)
      @uri = URI.parse(url)
    end

    DEFAULT_URI_COMPONENTS = {
      host: 'localhost',
      port: 8123
    }.freeze
    URI_COMPONENT_OPTIONS = %i[host port scheme].freeze
    QUERY_HASH_OPTIONS = %i[database password user].freeze

    def initialize_from_uri_components(**options)
      unless (unexpected_options = options.keys - URI_COMPONENT_OPTIONS - QUERY_HASH_OPTIONS).empty?
        fail ArgumentError, "Unexpected options – #{unexpected_options.join(', ')}"
      end
      query_hash = options.slice(*QUERY_HASH_OPTIONS)
      @uri = URI::HTTP.build(
        **DEFAULT_URI_COMPONENTS,
        query: (URI.encode_www_form(query_hash) unless query_hash.empty?),
        **options.slice(*URI_COMPONENT_OPTIONS),
      )
    end

    def uri_with_query(query)
      if query
        @uri.dup.tap do |uri|
          uri.query =
            URI.encode_www_form([(URI.decode_www_form(uri.query) if uri.query), query: query].compact.reduce(:merge))
        end
      else
        @uri
      end
    end

    def request(request_class, query, body)
      Net::HTTP.start(@uri.host, @uri.port) do |http|
        response = http.request(request_class.new(uri_with_query(query)), body)
        fail response.body unless response.is_a?(Net::HTTPSuccess)
        # https://github.com/yandex/ClickHouse/issues/2976
        response.body.force_encoding('ASCII-8BIT')
      end
    end
  end
end
