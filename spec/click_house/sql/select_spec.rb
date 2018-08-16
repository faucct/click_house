# frozen_string_literal: true

RSpec.describe ClickHouse::SQL::Select do
  subject { described_class.call { |select| select.expressions(:dummy).from(:one, database: :system) } }

  it { is_expected.to eq('SELECT dummy FROM system.one') }
end
