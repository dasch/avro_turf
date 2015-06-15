describe Symbol, "#as_avro" do
  it "returns the String representation of the Symbol" do
    expect(:hello.as_avro).to eq("hello")
  end
end
