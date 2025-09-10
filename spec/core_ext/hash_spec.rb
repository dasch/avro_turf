# frozen_string_literal: true

describe Hash, "#as_avro" do
  it "coerces the keys and values to Avro" do
    x = double(as_avro: "x")
    y = double(as_avro: "y")

    expect({ x => y }.as_avro).to eq({ "x" => "y" })
  end
end
