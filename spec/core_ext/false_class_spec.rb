describe FalseClass, "#as_avro" do
  it "returns itself" do
    expect(false.as_avro).to eq false
  end
end
