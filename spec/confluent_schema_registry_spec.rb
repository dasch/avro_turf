require 'webmock/rspec'
require 'avro_turf/confluent_schema_registry'
require 'avro_turf/test/fake_confluent_schema_registry_server'

describe AvroTurf::ConfluentSchemaRegistry do
  let(:client_cert) { "test client cert" }
  let(:client_key) { "test client key" }
  let(:client_key_pass) { "test client key password" }

  it_behaves_like "a confluent schema registry client" do
    let(:registry) {
      described_class.new(
        registry_url,
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass
      )
    }
  end
end
