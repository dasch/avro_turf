# frozen_string_literal: true

describe Numeric, "#as_avro" do
  it "returns the number itself" do
    expect(42.as_avro).to eq 42
    expect(4.2.as_avro).to eq 4.2
  end
end
