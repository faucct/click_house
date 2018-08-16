# frozen_string_literal: true

RSpec.describe ClickHouse::SQL::Insert do
  describe '.insert' do
    let(:http_interface) { ClickHouse::HTTPInterface.new }

    shared_context 'single column table' do |type|
      before do
        http_interface.post query: "CREATE TABLE foo (bar #{type}) ENGINE = TinyLog"
      end
      after { http_interface.post query: 'DROP TABLE foo' }

      def insert(value)
        http_interface.post query: described_class.call(into: :foo, rows: [[value]])
      end

      def table_tsv
        http_interface.get(query: 'SELECT * FROM foo').chomp("\n")
      end
    end

    context 'when there is a string containing quote' do
      include_context 'single column table', 'String'

      it { expect { insert "'" }.to change { table_tsv }.to("\\'") }
    end

    context 'when there is an array containing string' do
      include_context 'single column table', 'Array(String)'

      it { expect { insert ['a'] }.to change { table_tsv }.to("['a']") }
    end

    context 'when there is a null nullable string' do
      include_context 'single column table', 'Nullable(String)'

      it { expect { insert nil }.to change { table_tsv }.to('\N') }
    end

    context 'when there is an integer' do
      include_context 'single column table', 'Int8'

      it { expect { insert 42 }.to change { table_tsv }.to('42') }
    end

    context 'when there is a float' do
      include_context 'single column table', 'Float32'

      it { expect { insert 0.5 }.to change { table_tsv }.to('0.5') }
    end

    context 'when there is a date' do
      include_context 'single column table', 'Date'

      it { expect { insert Date.new(1994, 3, 14) }.to change { table_tsv }.to('1994-03-14') }
    end

    context 'when there is a time' do
      include_context 'single column table', 'DateTime'

      it { expect { insert Time.new(1994, 3, 14, 12, 34, 56) }.to change { table_tsv }.to('1994-03-14 12:34:56') }
    end

    context 'when there is a enum' do
      include_context 'single column table', "Enum8('hello' = 1, 'world' = 2)"

      it { expect { insert 'world' }.to change { table_tsv }.to('world') }
    end

    context 'when there is a nested data structure' do
      include_context 'single column table', 'Nested(tag String)'

      it { expect { insert %w[foo bar] }.to change { table_tsv }.to("['foo','bar']") }
    end
  end
end
