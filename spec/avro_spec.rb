describe AvroTurf do
  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/") }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  it "encodes and decodes data using a named schema" do
    define_schema "person.avsc", <<-AVSC
      {
        "name": "person",
        "type": "record",
        "fields": [
          {
            "type": "string",
            "name": "full_name"
          },
          {
            "name": "address",
            "type": {
              "type": "record",
              "name": "address",
              "fields": [
                {
                  "type": "string",
                  "name": "street"
                },
                {
                  "type": "string",
                  "name": "city"
                }
              ]
            }
          }
        ]
      }
    AVSC

    data = {
      "full_name" => "John Doe",
      "address" => {
        "street" => "Market st. 989",
        "city" => "San Francisco"
      }
    }

    encoded_data = avro.encode(data, schema_name: "person")

    expect(avro.decode(encoded_data, schema_name: "person")).to eq(data)
  end

  it "allows decoding without specifying a reader schema" do
    define_schema "message.avsc", <<-AVSC
      {
        "name": "message",
        "type": "string"
      }
    AVSC

    encoded_data = avro.encode("hello, world", schema_name: "message")

    expect(avro.decode(encoded_data)).to eq "hello, world"
  end
end
