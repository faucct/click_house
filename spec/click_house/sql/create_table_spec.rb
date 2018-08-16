RSpec.describe ClickHouse::SQL do
  describe '.create_table' do
    subject do
      lambda do
        http_interface.post query: described_class.create_table(name: :foo, columns: { bar: :String }, engine: :TinyLog)
      end
    end
    after { http_interface.post query: 'DROP TABLE foo' }
    let(:http_interface) { ClickHouse::HTTPInterface.new }

    it do
      is_expected.to change { TSV.parse(http_interface.get(query: 'SHOW TABLES')).without_header.map(&:first) }
        .to include('foo')
    end
  end
end
