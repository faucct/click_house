# frozen_string_literal: true

RSpec.describe ClickHouse::SQL::CreateTable do
  describe '.call' do
    subject do
      lambda do
        http_interface.post query: (described_class.call do |create_table|
          create_table.name(:foo).engine(&:tiny_log).columns { |columns| columns.call(:bar, :String) }
        end)
      end
    end
    after { http_interface.post query: 'DROP TABLE foo' }
    let(:http_interface) { ClickHouse::HTTPInterface.new }

    it { is_expected.to change { http_interface.get(query: 'SHOW TABLES') }.to include('foo') }
  end
end
