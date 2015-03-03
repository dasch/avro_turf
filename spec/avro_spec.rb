require 'fakefs/spec_helpers'

describe AvroTurf do
  include FakeFS::SpecHelpers

  it "encodes and decodes data using a named schema" do
    avro = AvroTurf.new(schemas_path: "spec/schemas/")

    schema = <<-AVSC
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

    FileUtils.mkdir_p("spec/schemas")

    File.open("spec/schemas/person.avsc", "w") do |f|
      f.write(schema)
    end

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
end
