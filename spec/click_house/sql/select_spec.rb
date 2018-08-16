RSpec.describe ClickHouse::SQL do
  describe '.select' do
    subject { described_class.select(expressions: :dummy, from: %i[system one]) }

    it { is_expected.to eq('SELECT dummy FROM system.one') }
  end
end
