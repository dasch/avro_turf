require 'fakefs/spec_helpers'

describe AvroTurf do
  include FakeFS::SpecHelpers

  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/") }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  it "encodes and decodes data using a named schema" do
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

  it "resolves named types" do
    File.open("spec/schemas/person.avsc", "w") do |f|
      f.write <<-AVSC
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
    end

    File.open("spec/schemas/address.avsc", "w") do |f|
      f.write <<-AVSC
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

  it "allows decoding without specifying a reader schema" do
    File.open("spec/schemas/message.avsc", "w") do |f|
      f.write <<-AVSC
        {
          "name": "message",
          "type": "string"
        }
      AVSC
    end

    encoded_data = avro.encode("hello, world", schema_name: "message")

    expect(avro.decode(encoded_data)).to eq "hello, world"
  end

  it "allows using namespaces in schemas" do
    FileUtils.mkdir_p("spec/schemas/test/people")

    File.open("spec/schemas/test/people/person.avsc", "w") do |f|
      f.write <<-AVSC
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
    end

    File.open("spec/schemas/test/people/address.avsc", "w") do |f|
      f.write <<-AVSC
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
    end

    data = {
      "full_name" => "John Doe",
      "address" => {
        "street" => "Market st. 989",
        "city" => "San Francisco"
      }
    }

    encoded_data = avro.encode(data, schema_name: "test.people.person")

    expect(avro.decode(encoded_data, schema_name: "test.people.person")).to eq(data)
  end

  it "raises AvroTurf::SchemaError if the schema's namespace doesn't match the file location" do
    FileUtils.mkdir_p("spec/schemas/test/people")

    File.open("spec/schemas/test/people/person.avsc", "w") do |f|
      f.write <<-AVSC
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
    end

    data = { "full_name" => "John Doe" }

    expect {
      avro.encode(data, schema_name: "test.people.person")
    }.to raise_error(AvroTurf::SchemaError, "expected schema `spec/schemas/test/people/person.avsc' to define type `test.people.person'")
  end
end
