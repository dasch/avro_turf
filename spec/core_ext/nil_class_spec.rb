using AvroTurf::CoreExt

describe NilClass, "#as_avro" do
  it "returns itself" do
    expect(nil.as_avro).to eq nil
  end
end
