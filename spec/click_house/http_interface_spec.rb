# frozen_string_literal: true

RSpec.describe ClickHouse::HTTPInterface do
  let(:connection) { described_class.new }

  describe '#get' do
    subject { connection.get(query: 'SHOW databases') }

    it { is_expected.to eq("default\nsystem\n") }
  end

  describe '#post' do
    subject { connection.post(query: 'CREATE TABLE foo (bar String) Engine = TinyLog') }
    after { connection.post(query: 'DROP TABLE IF EXISTS foo') }

    it { is_expected.to eq('') }
  end
end
