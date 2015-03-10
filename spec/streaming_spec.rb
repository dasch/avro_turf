describe AvroTurf, "#encode_to_stream" do
  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/") }

  before do
    FileUtils.mkdir_p("spec/schemas")

    define_schema "message.avsc", <<-AVSC
      {
        "name": "message",
        "type": "string"
      }
    AVSC
  end

  it "writes encoded data to an existing stream" do
    stream = StringIO.new
    avro.encode_to_stream("hello", stream: stream, schema_name: "message")

    expect(avro.decode(stream.string)).to eq "hello"
  end
end
