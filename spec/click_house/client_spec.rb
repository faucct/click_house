RSpec.describe ClickHouse::Client do
  let(:client) { described_class.new(http_interface: http_interface) }
  let(:http_interface) { ClickHouse::HTTPInterface.new }

  describe '#each_selected_row' do
    shared_context 'single value row' do |of_type:, with_value_sql:|
      before do
        http_interface.post query: "CREATE TABLE foo (bar #{of_type}) ENGINE = TinyLog"
        http_interface.post query: "INSERT INTO foo VALUES (#{with_value_sql})"
      end
      after { http_interface.post query: 'DROP TABLE foo' }

      shared_examples 'it selects' do |value:|
        it { expect(client.select_rows(expressions: :*, from: :foo)).to eq([[value]]) }
      end
    end

    context 'when there is a string containing quote' do
      include_context 'single value row', of_type: 'String', with_value_sql: "'\\''"

      it_behaves_like 'it selects', value: "'"
    end

    context 'when there is an array containing string' do
      include_context 'single value row', of_type: 'Array(String)', with_value_sql: "['a']"

      it_behaves_like 'it selects', value: ['a']
    end

    context 'when there is a null nullable string' do
      include_context 'single value row', of_type: 'Nullable(String)', with_value_sql: 'NULL'

      it_behaves_like 'it selects', value: nil
    end

    context 'when there is an integer' do
      include_context 'single value row', of_type: 'Int8', with_value_sql: '42'

      it_behaves_like 'it selects', value: 42
    end

    context 'when there is a float' do
      include_context 'single value row', of_type: 'Float32', with_value_sql: '0.5'

      it_behaves_like 'it selects', value: 0.5
    end

    context 'when there is a date' do
      include_context 'single value row', of_type: 'Date', with_value_sql: "'1994-03-14'"

      it_behaves_like 'it selects', value: Date.new(1994, 3, 14)
    end

    context 'when there is a time' do
      include_context 'single value row', of_type: 'DateTime', with_value_sql: "'1994-03-14 12:34:56'"

      it_behaves_like 'it selects', value: Time.new(1994, 3, 14, 12, 34, 56)
    end

    context 'when there is a enum' do
      include_context 'single value row', of_type: "Enum8('hello' = 1, 'world' = 2)", with_value_sql: "'world'"

      it_behaves_like 'it selects', value: 'world'
    end

    context 'when there is a nested data structure' do
      include_context 'single value row', of_type: 'Nested(tag String)', with_value_sql: "['foo', 'bar']"

      it_behaves_like 'it selects', value: %w[foo bar]
    end
  end
end
