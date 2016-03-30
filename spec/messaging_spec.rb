require 'webmock/rspec'
require 'avro_turf/messaging'
require_relative 'fake_schema_registry_server'

# active_support/core_ext is required only to defensively test
# against how it interferes with JSON encoding
require 'active_support'
require 'active_support/core_ext'

describe AvroTurf::Messaging do
  let(:registry_url) { "http://registry.example.com" }
  let(:logger) { Logger.new(StringIO.new) }

  let(:avro) {
    AvroTurf::Messaging.new(
      registry_url: registry_url,
      schemas_path: "spec/schemas",
      logger: logger
    )
  }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  before do
    stub_request(:any, /^#{registry_url}/).to_rack(FakeSchemaRegistryServer)
    FakeSchemaRegistryServer.clear
  end

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

  it "encodes and decodes messages" do
    message = { "full_name" => "John Doe" }
    data = avro.encode(message, schema_name: "person")
    expect(avro.decode(data)).to eq message
  end

  it "allows specifying a reader's schema" do
    message = { "full_name" => "John Doe" }
    data = avro.encode(message, schema_name: "person")
    expect(avro.decode(data, schema_name: "person")).to eq message
  end
end
