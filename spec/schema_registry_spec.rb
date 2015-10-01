require 'webmock/rspec'
require 'avro_turf/schema_registry'
require_relative 'fake_schema_registry_server'

describe AvroTurf::SchemaRegistry do
  let(:registry_url) { "http://registry.example.com" }

  before do
    stub_request(:any, /^#{registry_url}/).to_rack(FakeSchemaRegistryServer)
    FakeSchemaRegistryServer.clear
  end

  it "allows registering a schema" do
    logger = Logger.new(StringIO.new)
    registry = described_class.new(registry_url, logger: logger)

    schema = <<-JSON
      {
        "type": "record",
        "name": "person",
        "fields": [
          { "name": "name", "type": "string" }
        ]
      }
    JSON

    id = registry.register("some-subject", schema)
    fetched_schema = registry.fetch(id)

    expect(JSON.parse(fetched_schema)).to eq JSON.parse(schema)
  end
end
