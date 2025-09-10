# frozen_string_literal: true

describe String, "#as_avro" do
  it "returns itself" do
    expect("hello".as_avro).to eq "hello"
  end
end
