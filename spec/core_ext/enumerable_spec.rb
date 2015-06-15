describe Enumerable, "#as_avro" do
  it "returns an array" do
    expect(Set.new.as_avro).to eq []
  end

  it "coerces the items to Avro" do
    x = double(as_avro: "x")
    y = double(as_avro: "y")

    expect([x, y].as_avro).to eq ["x", "y"]
  end
end
