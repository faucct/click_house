# frozen_string_literal: true

RSpec.describe ClickHouse::SQL::Insert do
  describe '.insert' do
    let(:http_interface) { ClickHouse::HTTPInterface.new }

    shared_context 'single value row' do |value:, of_type:|
      before do
        http_interface.post query: "CREATE TABLE foo (bar #{of_type}) ENGINE = TinyLog"
      end
      after { http_interface.post query: 'DROP TABLE foo' }

      shared_examples 'it gets inserted' do |as:|
        it do
          expect do
            http_interface.post query: described_class.call(into: :foo, rows: [[value]])
          end.to change { http_interface.get(query: 'SELECT * FROM foo').chomp("\n") }.to(as)
        end
      end
    end

    context 'when there is a string containing quote' do
      include_context 'single value row', value: "'", of_type: 'String'

      it_behaves_like 'it gets inserted', as: "\\'"
    end

    context 'when there is an array containing string' do
      include_context 'single value row', value: ['a'], of_type: 'Array(String)'

      it_behaves_like 'it gets inserted', as: "['a']"
    end

    context 'when there is a null nullable string' do
      include_context 'single value row', value: nil, of_type: 'Nullable(String)'

      it_behaves_like 'it gets inserted', as: '\N'
    end

    context 'when there is an integer' do
      include_context 'single value row', value: 42, of_type: 'Int8'

      it_behaves_like 'it gets inserted', as: '42'
    end

    context 'when there is a float' do
      include_context 'single value row', value: 0.5, of_type: 'Float32'

      it_behaves_like 'it gets inserted', as: '0.5'
    end

    context 'when there is a date' do
      include_context 'single value row', value: Date.new(1994, 3, 14), of_type: 'Date'

      it_behaves_like 'it gets inserted', as: '1994-03-14'
    end

    context 'when there is a time' do
      include_context 'single value row', value: Time.new(1994, 3, 14, 12, 34, 56), of_type: 'DateTime'

      it_behaves_like 'it gets inserted', as: '1994-03-14 12:34:56'
    end

    context 'when there is a enum' do
      include_context 'single value row', value: 'world', of_type: "Enum8('hello' = 1, 'world' = 2)"

      it_behaves_like 'it gets inserted', as: 'world'
    end

    context 'when there is a nested data structure' do
      include_context 'single value row', value: %w[foo bar], of_type: 'Nested(tag String)'

      it_behaves_like 'it gets inserted', as: "['foo','bar']"
    end
  end
end
