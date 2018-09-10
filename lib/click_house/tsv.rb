module ClickHouse
  # @see https://clickhouse.yandex/docs/en/interfaces/formats/#tabseparated
  module TSV
    module AbstractParser
      def pattern
        fail NotImplementedError
      end

      def parse(_string)
        fail NotImplementedError
      end
    end

    class ArrayParser
      include AbstractParser

      def initialize(item_parser)
        @item_parser = item_parser
      end

      def pattern
        /\[\]|\[(?:#{@item_parser.pattern},)*#{@item_parser.pattern}\]/
      end

      def parse(string)
        items = []
        string[1..-2].scan(/(?:^|,)#{@item_parser.pattern}(?:$|,)/) { |match| items << @item_parser.parse(match) }
        items
      end
    end

    class NullableParser
      include AbstractParser

      def initialize(item_parser)
        @item_parser = item_parser
      end

      def pattern
        /\\N|#{@item_parser.pattern}/
      end

      def parse(string)
        @item_parser.parse(string) unless string == '\N'
      end
    end

    module IntParser
      extend AbstractParser

      def self.pattern
        /\d+/
      end

      def self.parse(string)
        string.to_i
      end
    end

    module FloatParser
      extend AbstractParser

      def self.pattern
        /\d+(?:\.\d+)?/
      end

      def self.parse(string)
        string.to_f
      end
    end

    module DateParser
      extend AbstractParser

      def self.pattern
        /\d+-\d+-\d+/
      end

      def self.parse(string)
        Date.strptime(string, '%Y-%m-%d')
      end
    end

    module DateTimeParser
      extend AbstractParser

      def self.pattern
        /\d+-\d+-\d+ \d+:\d+:\d+/
      end

      def self.parse(string)
        Time.strptime(string, '%Y-%m-%d %H:%M:%S')
      end
    end

    module NothingParser
      extend AbstractParser

      def self.pattern
        //
      end

      def self.parse(_string)
      end
    end

    module StringParser
      extend AbstractParser

      def self.pattern
        /(?:[^']|\\')*|'(?:[^']|\\')*'/
      end

      def self.parse(string)
        string
          .gsub(/^'(.*)'$/, '\1')
          .gsub(/#{STRING_SUBSTITUTIONS.each_key.map(&Regexp.method(:escape)).join('|')}/, STRING_SUBSTITUTIONS)
          .tap { |unescaped| unescaped.force_encoding Encoding.default_internal if Encoding.default_internal }
      end
    end

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

    def self.rows_enum(chunks_enum)
      Enumerator.new do |row_yielder|
        line_yielder = ->(line) { row_yielder << line.split("\t", -1) }
        final_buffer = chunks_enum.inject(''.b) do |buffer, chunk|
          *lines, new_buffer = (buffer + chunk).split("\n", -1)
          lines.each(&line_yielder)
          new_buffer
        end
        line_yielder.call final_buffer unless final_buffer.empty?
      end
    end

    def self.typed_parser(type)
      case type
      when /^Array\((.+)\)$/
        item_parser = typed_parser(Regexp.last_match[1])
        ->(value) { value.match(/^\[(.*)\]$/)[1].split(',').map(&item_parser) }
      when /^Nullable\((.+)\)$/
        not_null_parser = typed_parser(Regexp.last_match[1])
        ->(value) { not_null_parser.call(value) unless value == '\N' }
      when /^U?Int(?:8|16|32|64)$/ then
        :to_i
      when /^Float(?:32|64)$/ then
        :to_f
      when /^Enum(?:8|16)\(.+\)$/, /^FixedString\(\d+\)$/, 'String'
        lambda do |escaped|
          escaped
            .gsub(/#{STRING_SUBSTITUTIONS.each_key.map(&Regexp.method(:escape)).join('|')}/, STRING_SUBSTITUTIONS)
            .gsub(/\A'(.*)'\z/, '\1')
            .tap { |unescaped| unescaped.force_encoding Encoding.default_internal if Encoding.default_internal }
        end
      when 'Date' then
        ->(value) { Date.strptime(value, '%Y-%m-%d') }
      when 'DateTime' then
        ->(value) { Time.strptime(value, '%Y-%m-%d %H:%M:%S') }
      when 'Nothing' then
        ->(_) {}
      else
        fail NotImplementedError, "Unknown type #{type.inspect}"
      end.to_proc
    end

    def self.typed_pattern_parser(type)
      case type
      when /^Array\((.+)\)$/ then ArrayParser.new(typed_pattern_parser(Regexp.last_match[1]))
      when /^Nullable\((.+)\)$/ then NullableParser.new(typed_pattern_parser(Regexp.last_match[1]))
      when /^U?Int(?:8|16|32|64)$/ then IntParser
      when /^Float(?:32|64)$/ then FloatParser
      when /^Enum(?:8|16)\(.+\)$/, /^FixedString\(\d+\)$/, 'String' then StringParser
      when 'Date' then DateParser
      when 'DateTime' then DateTimeParser
      when 'Nothing' then NothingParser
      else fail NotImplementedError, "Unknown type #{type.inspect}"
      end
    end
  end
end
