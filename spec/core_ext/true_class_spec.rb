# frozen_string_literal: true

describe TrueClass, "#as_avro" do
  it "returns itself" do
    expect(true.as_avro).to eq true
  end
end
