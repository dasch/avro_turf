describe AvroTurf do
  let(:log) { StringIO.new }
  let(:logger) { Logger.new(log) }
  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/", logger: logger) }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  describe "#encode" do
    it "encodes data with Avro" do
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
      reader_avro = AvroTurf.new(schemas_path: "spec/schemas/reader", logger: logger)

      expect(reader_avro.decode(encoded_data, schema_name: "point")).to eq({ "x" => 42 })
    end

    it "automatically uses a local reader schema if the writer schema has a name" do
      FileUtils.mkdir_p("spec/schemas/writer")
      FileUtils.mkdir_p("spec/schemas/reader")

      define_schema "writer/person.avsc", <<-AVSC
        {
          "name": "person",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "full_name"
            },
            {
              "type": "long",
              "name": "age"
            }
          ]
        }
      AVSC

      define_schema "reader/person.avsc", <<-AVSC
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

      data = {
        "full_name" => "John Doe",
        "age" => 42
      }

      writer_avro = AvroTurf.new(schemas_path: "spec/schemas/writer", logger: logger)
      reader_avro = AvroTurf.new(schemas_path: "spec/schemas/reader", logger: logger)

      encoded_data = writer_avro.encode(data, schema_name: "person")

      # The reader ignores the `age` field.
      expect(reader_avro.decode(encoded_data)).to eq({ "full_name" => "John Doe" })
    end

    it "logs a warning when decoding using the writer's schema" do
      define_schema "message.avsc", <<-AVSC
        {
          "name": "message",
          "type": "record",
          "fields": [
            { "name": "message", "type": "string" }
          ]
        }
      AVSC

      encoded_data = avro.encode({ "message" => "hello, world" }, schema_name: "message")

      # Use a different schemas path when reading schemas. Otherwise, the writer's schema
      # will be available when decoding.
      FileUtils.mkdir_p("spec/schemas/reader")
      reader_avro = AvroTurf.new(schemas_path: "spec/schemas/reader", logger: logger)

      reader_avro.decode(encoded_data)

      expect(log.string).to include "Could not find schema `message' locally; using writer's schema instead"
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
