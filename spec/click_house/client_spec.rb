# frozen_string_literal: true

RSpec.describe ClickHouse::Client do
  let(:client) { described_class.new(http_interface: http_interface) }
  let(:http_interface) { ClickHouse::HTTPInterface.new }

  describe '#each_selected_row' do
    shared_context 'selecting one' do |sql|
      subject { client.select_rows { |select| select.expressions(sql).from(:one, database: :system) }[0][0] }
    end

    context 'when value is a string containing quote' do
      include_context 'selecting one', "'\\''"

      it { is_expected.to eq("'") }
    end

    context 'when value is an array containing string' do
      include_context 'selecting one', "['a']"

      it { is_expected.to eq(['a']) }
    end

    context 'when value is a null string' do
      include_context 'selecting one', 'CAST(NULL AS Nullable(String))'

      it { is_expected.to eq(nil) }
    end

    context 'when value is nothing' do
      include_context 'selecting one', 'NULL'

      it { is_expected.to eq(nil) }
    end

    context 'when value is an integer' do
      include_context 'selecting one', '42'

      it { is_expected.to eq(42) }
    end

    context 'when value is a float' do
      include_context 'selecting one', '0.5'

      it { is_expected.to eq(0.5) }
    end

    context 'when value is a date' do
      include_context 'selecting one', "toDate('1994-03-14')"

      it { is_expected.to eq(Date.new(1994, 3, 14)) }
    end

    context 'when value is a date-time' do
      include_context 'selecting one', "toDateTime('1994-03-14 12:34:56')"

      it { is_expected.to eq(DateTime.new(1994, 3, 14, 12, 34, 56)) }
    end

    context 'when value is a enum' do
      include_context 'selecting one', "CAST('world' AS Enum8('hello' = 1, 'world' = 2))"

      it { is_expected.to eq('world') }
    end
  end
end
