require 'fakefs/spec_helpers'

describe AvroTurf do
  include FakeFS::SpecHelpers

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

  it "resolves named types" do
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
            "type": "address"
          }
        ]
      }
    AVSC

    define_schema "address.avsc", <<-AVSC
      {
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

  it "allows using namespaces in schemas" do
    FileUtils.mkdir_p("spec/schemas/test/people")

    define_schema "test/people/person.avsc", <<-AVSC
      {
        "name": "person",
        "namespace": "test.people",
        "type": "record",
        "fields": [
          {
            "type": "string",
            "name": "full_name"
          },
          {
            "name": "address",
            "type": "test.people.address"
          }
        ]
      }
    AVSC

    define_schema "test/people/address.avsc", <<-AVSC
      {
        "name": "address",
        "namespace": "test.people",
        "type": "record",
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
    AVSC

    data = {
      "full_name" => "John Doe",
      "address" => {
        "street" => "Market st. 989",
        "city" => "San Francisco"
      }
    }

    encoded_data = avro.encode(data, schema_name: "person", namespace: "test.people")

    expect(avro.decode(encoded_data, schema_name: "person", namespace: "test.people")).to eq(data)
  end

  it "raises AvroTurf::SchemaError if the schema's namespace doesn't match the file location" do
    FileUtils.mkdir_p("spec/schemas/test/people")

    define_schema "test/people/person.avsc", <<-AVSC
      {
        "name": "person",
        "namespace": "yoyoyo.nanana",
        "type": "record",
        "fields": [
          {
            "type": "string",
            "name": "full_name"
          }
        ]
      }
    AVSC

    data = { "full_name" => "John Doe" }

    expect {
      avro.encode(data, schema_name: "test.people.person")
    }.to raise_error(AvroTurf::SchemaError, "expected schema `spec/schemas/test/people/person.avsc' to define type `test.people.person'")
  end

  it "caches schemas in memory" do
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

    # Warm the schema cache.
    avro.encode(data, schema_name: "person")

    # Force a failure if the schema file is read again.
    FileUtils.rm("spec/schemas/person.avsc")

    encoded_data = avro.encode(data, schema_name: "person")

    expect(avro.decode(encoded_data, schema_name: "person")).to eq(data)
  end

  def define_schema(path, content)
    File.open(File.join("spec/schemas", path), "w") do |f|
      f.write(content)
    end
  end
end
