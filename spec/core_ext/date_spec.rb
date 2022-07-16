using AvroTurf::CoreExt

describe Date, "#as_avro" do
  it "returns an ISO8601 string describing the time" do
    date = Date.today
    expect(date.as_avro).to eq(date.iso8601)
  end
end
