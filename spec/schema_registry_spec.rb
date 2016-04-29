require 'webmock/rspec'
require 'avro_turf/schema_registry'
require_relative 'fake_schema_registry_server'

describe AvroTurf::SchemaRegistry do
  it_behaves_like "a schema registry client" do
    let(:registry) { described_class.new(registry_url, logger: logger) }
  end
end
