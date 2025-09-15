# frozen_string_literal: true

describe NilClass, "#as_avro" do
  it "returns itself" do
    expect(nil.as_avro).to eq nil
  end
end
