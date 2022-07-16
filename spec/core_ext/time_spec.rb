using AvroTurf::CoreExt

describe Time, "#as_avro" do
  it "returns an ISO8601 string describing the time" do
    time = Time.now
    expect(time.as_avro).to eq(time.iso8601)
  end
end
