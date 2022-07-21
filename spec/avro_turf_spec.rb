describe AvroTurf do
  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/") }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  describe "#encode" do
    context "when using plain schema" do
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

      it "allows specifying a codec that should be used to compress messages" do
        compressed_avro = AvroTurf.new(schemas_path: "spec/schemas/", codec: "deflate")

        data = {
          "full_name" => "John Doe" * 100
        }

        uncompressed_data = avro.encode(data, schema_name: "person")
        compressed_data = compressed_avro.encode(data, schema_name: "person")

        expect(compressed_data.bytesize).to be < uncompressed_data.bytesize
        expect(compressed_avro.decode(compressed_data)).to eq(data)
      end
    end

    context 'when using nested schemas' do
      before do
        define_schema "post.avsc", <<-AVSC
          {
            "name": "post",
            "type": "record",
            "fields": [
              {
                "name": "tag",
                "type": {
                  "type": "enum",
                  "name": "tag",
                  "symbols": ["foo", "bar"]
                }
              },
              {
                "name": "messages",
                "type": {
                  "type": "array",
                  "items": "message"
                }
              },
              {
                "name": "status",
                "type": "publishing_status"
              }
            ]
          }
        AVSC

        define_schema "publishing_status.avsc", <<-AVSC
          {
            "name": "publishing_status",
            "type": "enum",
            "symbols": ["draft", "published", "archived"]
          }
        AVSC

        define_schema "message.avsc", <<-AVSC
          {
            "name": "message",
            "type": "record",
            "fields": [
              {
                "type": "string",
                "name": "content"
              },
              {
                "name": "label",
                "type": {
                  "type": "enum",
                  "name": "label",
                  "symbols": ["foo", "bar"]
                }
              },
              {
                "name": "status",
                "type": "publishing_status"
              }
            ]
          }
        AVSC
      end

      it "encodes data with Avro" do
        data = {
          "tag" => "foo",
          "messages" => [
            {
              "content" => "hello",
              "label" => "bar",
              "status" => "draft"
            }
          ],
          "status" => "published"
        }

        encoded_data = avro.encode(data, schema_name: "post")

        expect(avro.decode(encoded_data)).to eq(data)
      end
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

    context "validating" do
      subject(:encode_to_stream) do
        stream = StringIO.new
        avro.encode_to_stream(message, stream: stream, schema_name: "message", validate: true)
      end

      context "with a valid message" do
        let(:message) { { "full_name" => "John Doe" } }

        it "does not raise any error" do
          define_schema "message.avsc", <<-AVSC
            {
              "name": "message",
              "type": "record",
              "fields": [
                { "name": "full_name", "type": "string" }
              ]
            }
          AVSC

          expect { encode_to_stream }.not_to raise_error
        end
      end

      context "when message has wrong type" do
        let(:message) { { "full_name" => 123 } }

        it "raises Avro::SchemaValidator::ValidationError with a message about type mismatch" do
          define_schema "message.avsc", <<-AVSC
            {
              "name": "message",
              "type": "record",
              "fields": [
                { "name": "full_name", "type": "string" }
              ]
            }
          AVSC

          expect { encode_to_stream }.to raise_error(Avro::SchemaValidator::ValidationError, /\.full_name expected type string, got int/)
        end
      end

      context "when message contains extra fields (typo in key)" do
        let(:message) { { "fulll_name" => "John Doe" } }

        it "raises Avro::SchemaValidator::ValidationError with a message about extra field" do
          define_schema "message.avsc", <<-AVSC
            {
              "name": "message",
              "type": "record",
              "fields": [
                { "name": "full_name", "type": "string" }
              ]
            }
          AVSC

          expect { encode_to_stream }.to raise_error(Avro::SchemaValidator::ValidationError, /extra field 'fulll_name'/)
        end
      end

      context "when message contains extra fields" do
        let(:message) { { "full_name" => "John Doe", "first_name" => "John", "last_name" => "Doe" } }
        subject(:encode_to_stream) do
          stream = StringIO.new
          avro.encode_to_stream(message, stream: stream, schema_name: "message",
                                validate: true,
                                validate_options: { recursive: true, encoded: false, fail_on_extra_fields: false }
          )
        end

        it "should not raise Avro::SchemaValidator::ValidationError with a message about extra field" do
          define_schema "message.avsc", <<-AVSC
            {
              "name": "message",
              "type": "record",
              "fields": [
                { "name": "full_name", "type": "string" }
              ]
            }
          AVSC

          expect { encode_to_stream }.not_to raise_error
        end
      end
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

  describe "#valid?" do
    before do
      define_schema "message.avsc", <<-AVSC
        {
          "name": "message",
          "type": "string"
        }
      AVSC
    end

    it "returns true if the datum matches the schema" do
      datum = "hello"
      expect(avro.valid?(datum, schema_name: "message")).to eq true
    end

    it "returns false if the datum does not match the schema" do
      datum = 42
      expect(avro.valid?(datum, schema_name: "message")).to eq false
    end

    it "handles symbol keys in hashes" do
      define_schema "postcard.avsc", <<-AVSC
        {
          "name": "postcard",
          "type": "record",
          "fields": [
            { "name": "message", "type": "string" }
          ]
        }
      AVSC

      datum = { message: "hello" }
      expect(avro.valid?(datum, schema_name: "postcard")).to eq true
    end
  end
end
