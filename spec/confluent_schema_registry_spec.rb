require 'webmock/rspec'
require 'avro_turf/confluent_schema_registry'
require 'avro_turf/test/fake_confluent_schema_registry_server'

describe AvroTurf::ConfluentSchemaRegistry do
  it_behaves_like "a confluent schema registry client" do
    let(:registry) { described_class.new(registry_url) }
  end
end
