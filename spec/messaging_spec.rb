require 'webmock/rspec'
require 'avro_turf/messaging'
require 'avro_turf/test/fake_confluent_schema_registry_server'

describe AvroTurf::Messaging do
  let(:registry_url) { "http://registry.example.com" }
  let(:logger) { Logger.new(StringIO.new) }
  let(:registry) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }

  let(:avro) {
    AvroTurf::Messaging.new(
      registry_url: registry_url,
      logger: logger
    )
  }

  let(:message) { { "full_name" => "John Doe" } }

  before do
    stub_request(:any, /^#{registry_url}/).to_rack(FakeConfluentSchemaRegistryServer)
    FakeConfluentSchemaRegistryServer.clear
  end

  before do
    schema_json = <<-AVSC
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
    registry.register('person', Avro::Schema.parse(schema_json))
    registry.register('people', Avro::Schema.parse(schema_json))
  end

  shared_examples_for "encoding and decoding" do
    it "encodes and decodes messages" do
      data = avro.encode(message, schema_name: "person")
      expect(avro.decode(data)).to eq message
    end

    it "allows specifying a reader's schema" do
      data = avro.encode(message, schema_name: "person")
      expect(avro.decode(data, schema_name: "person")).to eq message
    end

    it "caches parsed schemas for decoding" do
      data = avro.encode(message, schema_name: "person")
      avro.decode(data)
      allow(Avro::Schema).to receive(:parse).and_call_original
      expect(avro.decode(data)).to eq message
      expect(Avro::Schema).not_to have_received(:parse)
    end
  end

  it_behaves_like "encoding and decoding"

  context "with a provided registry" do
    let(:avro) do
      AvroTurf::Messaging.new(
        registry: registry,
        logger: logger
      )
    end

    it_behaves_like "encoding and decoding"

    it "uses the provided registry" do
      allow(registry).to receive(:subject_version).and_call_original
      message = { "full_name" => "John Doe" }
      avro.encode(message, schema_name: "person")
      expect(registry).to have_received(:subject_version).with('person')
    end

    it "allows specifying a schema registry subject" do
      allow(registry).to receive(:subject_version).and_call_original
      message = { "full_name" => "John Doe" }
      avro.encode(message, schema_name: 'person', subject: 'people')
      expect(registry).to have_received(:subject_version).with('people')
    end
  end
end
