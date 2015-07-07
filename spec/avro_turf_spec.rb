describe AvroTurf do
  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/") }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  describe "#encode" do
    before do
      define_schema "person.avsc", <<-AVSC
        {
          "name": "person",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "full_name"
            }
          ]
        }
      AVSC
    end

    it "encodes data with Avro" do
      data = {
        "full_name" => "John Doe"
      }

      encoded_data = avro.encode(data, schema_name: "person")

      expect(avro.decode(encoded_data)).to eq(data)
    end
  end

  describe "#decode" do
    it "decodes Avro data using the inlined writer's schema" do
      define_schema "message.avsc", <<-AVSC
        {
          "name": "message",
          "type": "string"
        }
      AVSC

      encoded_data = avro.encode("hello, world", schema_name: "message")

      expect(avro.decode(encoded_data)).to eq "hello, world"
    end

    it "decodes Avro data using a specified reader's schema" do
      FileUtils.mkdir_p("spec/schemas/reader")

      define_schema "point.avsc", <<-AVSC
        {
          "name": "point",
          "type": "record",
          "fields": [
            { "name": "x", "type": "long" },
            { "name": "y", "type": "long" }
          ]
        }
      AVSC

      define_schema "reader/point.avsc", <<-AVSC
        {
          "name": "point",
          "type": "record",
          "fields": [
            { "name": "x", "type": "long" }
          ]
        }
      AVSC

      encoded_data = avro.encode({ "x" => 42, "y" => 13 }, schema_name: "point")
      reader_avro = AvroTurf.new(schemas_path: "spec/schemas/reader")

      expect(reader_avro.decode(encoded_data, schema_name: "point")).to eq({ "x" => 42 })
    end
  end

  describe "#encode_to_stream" do
    it "writes encoded data to an existing stream" do
      define_schema "message.avsc", <<-AVSC
        {
          "name": "message",
          "type": "string"
        }
      AVSC

      stream = StringIO.new
      avro.encode_to_stream("hello", stream: stream, schema_name: "message")

      expect(avro.decode(stream.string)).to eq "hello"
    end
  end

  describe "#decode_stream" do
    it "decodes Avro data from a stream" do
      define_schema "message.avsc", <<-AVSC
        {
          "name": "message",
          "type": "string"
        }
      AVSC

      encoded_data = avro.encode("hello", schema_name: "message")
      stream = StringIO.new(encoded_data)

      expect(avro.decode_stream(stream)).to eq "hello"
    end
  end
end
