require 'webmock/rspec'
require 'avro_turf/messaging'
require 'avro_turf/test/fake_confluent_schema_registry_server'

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

  let(:message) { { "full_name" => "John Doe" } }
  let(:schema_json) do
    <<-AVSC
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

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  before do
    stub_request(:any, /^#{registry_url}/).to_rack(FakeConfluentSchemaRegistryServer)
    FakeConfluentSchemaRegistryServer.clear
  end

  before do
    define_schema "person.avsc", schema_json
  end

  shared_examples_for "encoding and decoding with the schema from schema store" do
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

  shared_examples_for 'encoding and decoding with the schema from registry' do
    before do
      registry = AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger)
      registry.register('person', Avro::Schema.parse(schema_json))
      registry.register('people', Avro::Schema.parse(schema_json))
    end

    it 'encodes and decodes messages' do
      data = avro.encode(message, subject: 'person', version: 1)
      expect(avro.decode(data)).to eq message
    end

    it "allows specifying a reader's schema by subject and version" do
      data = avro.encode(message, subject: 'person', version: 1)
      expect(avro.decode(data, schema_name: 'person')).to eq message
    end

    it 'raises AvroTurf::SchemaNotFoundError when the schema does not exist on registry' do
      expect { avro.encode(message, subject: 'missing', version: 1) }.to raise_error(AvroTurf::SchemaNotFoundError)
    end

    it 'caches parsed schemas for decoding' do
      data = avro.encode(message, subject: 'person', version: 1)
      avro.decode(data)
      allow(Avro::Schema).to receive(:parse).and_call_original
      expect(avro.decode(data)).to eq message
      expect(Avro::Schema).not_to have_received(:parse)
    end
  end

  it_behaves_like "encoding and decoding with the schema from schema store"

  it_behaves_like 'encoding and decoding with the schema from registry'

  context "with a provided registry" do
    let(:registry) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }

    let(:avro) do
      AvroTurf::Messaging.new(
        registry: registry,
        schemas_path: "spec/schemas",
        logger: logger
      )
    end

    it_behaves_like "encoding and decoding with the schema from schema store"

    it_behaves_like 'encoding and decoding with the schema from registry'

    it "uses the provided registry" do
      allow(registry).to receive(:register).and_call_original
      message = { "full_name" => "John Doe" }
      avro.encode(message, schema_name: "person")
      expect(registry).to have_received(:register).with("person", anything)
    end

    it "allows specifying a schema registry subject" do
      allow(registry).to receive(:register).and_call_original
      message = { "full_name" => "John Doe" }
      avro.encode(message, schema_name: "person", subject: "people")
      expect(registry).to have_received(:register).with("people", anything)
    end
  end

  context "with a provided schema store" do
    let(:schema_store) { AvroTurf::SchemaStore.new(path: "spec/schemas") }

    let(:avro) do
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        schema_store: schema_store,
        logger: logger
      )
    end

    it_behaves_like "encoding and decoding with the schema from schema store"

    it "uses the provided schema store" do
      allow(schema_store).to receive(:find).and_call_original
      avro.encode(message, schema_name: "person")
      expect(schema_store).to have_received(:find).with("person", nil)
    end
  end

  describe 'decoding with #decode_message' do
    shared_examples_for "encoding and decoding with the schema from schema store" do
      it "encodes and decodes messages" do
        data = avro.encode(message, schema_name: "person")
        result = avro.decode_message(data)
        expect(result.message).to eq message
        expect(result.schema_id).to eq 0
      end

      it "allows specifying a reader's schema" do
        data = avro.encode(message, schema_name: "person")
        expect(avro.decode_message(data, schema_name: "person").message).to eq message
      end

      it "caches parsed schemas for decoding" do
        data = avro.encode(message, schema_name: "person")
        avro.decode_message(data)
        allow(Avro::Schema).to receive(:parse).and_call_original
        expect(avro.decode_message(data).message).to eq message
        expect(Avro::Schema).not_to have_received(:parse)
      end
    end

    shared_examples_for 'encoding and decoding with the schema from registry' do
      before do
        registry = AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger)
        registry.register('person', Avro::Schema.parse(schema_json))
        registry.register('people', Avro::Schema.parse(schema_json))
      end

      it 'encodes and decodes messages' do
        data = avro.encode(message, subject: 'person', version: 1)
        result = avro.decode_message(data)
        expect(result.message).to eq message
        expect(result.schema_id).to eq 0
      end

      it "allows specifying a reader's schema by subject and version" do
        data = avro.encode(message, subject: 'person', version: 1)
        expect(avro.decode_message(data, schema_name: 'person').message).to eq message
      end

      it 'raises AvroTurf::SchemaNotFoundError when the schema does not exist on registry' do
        expect { avro.encode(message, subject: 'missing', version: 1) }.to raise_error(AvroTurf::SchemaNotFoundError)
      end

      it 'caches parsed schemas for decoding' do
        data = avro.encode(message, subject: 'person', version: 1)
        avro.decode_message(data)
        allow(Avro::Schema).to receive(:parse).and_call_original
        expect(avro.decode_message(data).message).to eq message
        expect(Avro::Schema).not_to have_received(:parse)
      end
    end

    it_behaves_like "encoding and decoding with the schema from schema store"

    it_behaves_like 'encoding and decoding with the schema from registry'

    context "with a provided registry" do
      let(:registry) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }

      let(:avro) do
        AvroTurf::Messaging.new(
          registry: registry,
          schemas_path: "spec/schemas",
          logger: logger
        )
      end

      it_behaves_like "encoding and decoding with the schema from schema store"

      it_behaves_like 'encoding and decoding with the schema from registry'

      it "uses the provided registry" do
        allow(registry).to receive(:register).and_call_original
        message = { "full_name" => "John Doe" }
        avro.encode(message, schema_name: "person")
        expect(registry).to have_received(:register).with("person", anything)
      end

      it "allows specifying a schema registry subject" do
        allow(registry).to receive(:register).and_call_original
        message = { "full_name" => "John Doe" }
        avro.encode(message, schema_name: "person", subject: "people")
        expect(registry).to have_received(:register).with("people", anything)
      end
    end

    context "with a provided schema store" do
      let(:schema_store) { AvroTurf::SchemaStore.new(path: "spec/schemas") }

      let(:avro) do
        AvroTurf::Messaging.new(
          registry_url: registry_url,
          schema_store: schema_store,
          logger: logger
        )
      end

      it_behaves_like "encoding and decoding with the schema from schema store"

      it "uses the provided schema store" do
        allow(schema_store).to receive(:find).and_call_original
        avro.encode(message, schema_name: "person")
        expect(schema_store).to have_received(:find).with("person", nil)
      end
    end
  end
end
