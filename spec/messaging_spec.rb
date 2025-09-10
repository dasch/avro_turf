# frozen_string_literal: true

require 'webmock/rspec'
require 'avro_turf/messaging'

describe AvroTurf::Messaging do
  let(:registry_url) { "http://registry.example.com" }
  let(:client_cert) { "test client cert" }
  let(:client_key) { "test client key" }
  let(:client_key_pass) { "test client key password" }
  let(:logger) { Logger.new(StringIO.new) }
  let(:path_prefix) { nil }
  let(:avro) {
    AvroTurf::Messaging.new(
      registry_url: registry_url,
      schemas_path: "spec/schemas",
      logger: logger,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass
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

  let(:city_message) { { "name" => "Paris" } }
  let(:city_schema_json) do
    <<-AVSC
      {
        "name": "city",
        "type": "record",
        "fields": [
          {
            "type": "string",
            "name": "name"
          }
        ]
      }
    AVSC
  end

  let(:city_schema) { Avro::Schema.parse(city_schema_json) }
  let(:schema) { Avro::Schema.parse(schema_json) }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  before do
    stub_request(:any, /^#{registry_url}/).to_rack(AuthorizedFakeConfluentSchemaRegistryServer)
    AuthorizedFakeConfluentSchemaRegistryServer.clear
  end

  before do
    define_schema "person.avsc", schema_json
    define_schema "city.avsc", city_schema_json
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
      registry = AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger, path_prefix: path_prefix)
      registry.register('person', schema)
      registry.register('people', schema)
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

    it 'raises AvroTurf::SchemaNotFoundError when the schema does not exist on registry and register_schemas false' do
      expect { avro.encode(city_message, schema_name: 'city', register_schemas: false) }.
        to raise_error(AvroTurf::SchemaNotFoundError, "Schema with structure: #{city_schema} not found on registry")
    end

    it 'encodes with register_schemas false when the schema exists on the registry' do
      data = avro.encode(message, schema_name: 'person', register_schemas: false)
      expect(avro.decode(data, schema_name: 'person')).to eq message
    end

    it 'caches parsed schemas for decoding' do
      data = avro.encode(message, subject: 'person', version: 1)
      avro.decode(data)
      allow(Avro::Schema).to receive(:parse).and_call_original
      expect(avro.decode(data)).to eq message
      expect(Avro::Schema).not_to have_received(:parse)
    end
  end

  shared_examples_for 'encoding and decoding with the schema_id from registry' do
    before do
      registry = AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger, path_prefix: path_prefix)
      registry.register('person', schema)
      registry.register('people', schema)
    end

    it 'encodes and decodes messages' do
      data = avro.encode(message, schema_id: 0)
      expect(avro.decode(data)).to eq message
    end

    it 'raises AvroTurf::SchemaNotFoundError when the schema does not exist on registry' do
      expect { avro.encode(message, schema_id: 5) }.to raise_error(AvroTurf::SchemaNotFoundError)
    end

    it 'caches parsed schemas for decoding' do
      data = avro.encode(message, schema_id: 0)
      avro.decode(data)
      allow(Avro::Schema).to receive(:parse).and_call_original
      expect(avro.decode(data)).to eq message
      expect(Avro::Schema).not_to have_received(:parse)
    end
  end

  it_behaves_like "encoding and decoding with the schema from schema store"

  it_behaves_like 'encoding and decoding with the schema from registry'

  it_behaves_like 'encoding and decoding with the schema_id from registry'

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

    it_behaves_like 'encoding and decoding with the schema_id from registry'

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
        expect(result.writer_schema).to eq schema
        expect(result.reader_schema).to eq nil
      end

      it "allows specifying a reader's schema" do
        data = avro.encode(message, schema_name: "person")
        result = avro.decode_message(data, schema_name: "person")
        expect(result.message).to eq message
        expect(result.writer_schema).to eq schema
        expect(result.reader_schema).to eq schema
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
        registry.register('person', schema)
        registry.register('people', schema)
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

  context "validating" do
    subject(:encode){ avro.encode(message, schema_name: "person", validate: true) }

    context "for correct message" do
      it { expect { encode }.not_to raise_error }
    end

    context "when message has wrong type" do
      let(:message) { { "full_name" => 123 } }

      it { expect { encode }.to raise_error(Avro::SchemaValidator::ValidationError, /\.full_name expected type string, got int/) }
    end

    context "when message contains extra fields (typo in key)" do
      let(:message) { { "fulll_name" => "John Doe" } }

      it { expect { encode }.to raise_error(Avro::SchemaValidator::ValidationError, /extra field 'fulll_name'/) }
    end
  end

  context 'fetching and registering schema' do
    let(:schema_store) { AvroTurf::SchemaStore.new(path: "spec/schemas") }

    let(:registry) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }

    let(:avro) do
      AvroTurf::Messaging.new(
        registry: registry,
        schema_store: schema_store,
        logger: logger
      )
    end

    let(:schema_id) { 234 }

    context 'using fetch_schema' do
      subject { avro.fetch_schema(subject: subj, version: version) }

      let(:subj) { 'subject' }

      let(:version) { 'version' }

      let(:response) { {'id' => schema_id, 'schema' => schema_json} }

      before do
        allow(registry).to receive(:subject_version).with(subj, version).and_return(response)
      end

      it 'gets schema from registry' do
        expect(subject).to eq([schema, schema_id])
      end

      context "with an incompatible schema type" do
        let(:response) { {'id' => schema_id, 'schema' => 'blah', 'schemaType' => schema_type } }
        let(:schema_type) { 'PROTOBUF' }

        it 'raises IncompatibleSchemaError' do
          expect { subject }.to raise_error(
            AvroTurf::IncompatibleSchemaError,
            "The #{schema_type} schema for #{subj} is incompatible."
          )
        end
      end
    end

    context 'using fetch_schema_by_id' do
      subject { avro.fetch_schema_by_id(schema_id) }

      before do
        allow(registry).to receive(:fetch).with(schema_id).and_return(schema_json)
      end

      it 'gets schema from registry' do
        expect(subject).to eq([schema, schema_id])
      end
    end

    context 'using fetch_schema_by_body' do
      let(:subject_name) { 'city' }
      let(:schema_name) { 'city' }
      let(:namespace) { 'namespace' }
      let(:city_schema_id) { 125 }
      let(:city_schema_data) do
        {
          "subject" => subject_name,
          "version" => 123,
          "id" => city_schema_id,
          "schema" => city_schema
        }
      end

      subject(:fetch_schema_by_body) do
        avro.fetch_schema_by_body(schema_name: schema_name, namespace: namespace, subject: subject_name)
      end

      before do
        allow(schema_store).to receive(:find).with(schema_name, namespace).and_return(city_schema)
        allow(registry).to receive(:check).with(subject_name, city_schema).and_return(city_schema_data)
      end

      it 'gets schema from registry' do
        expect(fetch_schema_by_body).to eq([city_schema, city_schema_id])
      end
    end

    context 'using register_schema' do
      let(:schema_name) { 'schema_name' }

      let(:namespace) { 'namespace' }

      before do
        allow(schema_store).to receive(:find).with(schema_name, namespace).and_return(schema)
      end

      context 'when subject is not set' do
        subject { avro.register_schema(schema_name: schema_name, namespace: namespace) }

        before do
          allow(registry).to receive(:register).with(schema.fullname, schema).and_return(schema_id)
        end

        it 'registers schema in registry' do
          expect(subject).to eq([schema, schema_id])
        end
      end

      context 'when subject is set' do
        subject { avro.register_schema(schema_name: schema_name, namespace: namespace, subject: subj) }

        let(:subj) { 'subject' }

        before do
          allow(registry).to receive(:register).with(subj, schema).and_return(schema_id)
        end

        it 'registers schema in registry' do
          expect(subject).to eq([schema, schema_id])
        end
      end
    end
  end

  context 'with a registry path prefix' do
    let(:path_prefix) { '/prefix' }

    let(:avro) {
      AvroTurf::Messaging.new(
        registry_path_prefix: path_prefix,
        registry_url: registry_url,
        schemas_path: "spec/schemas",
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass
      )
    }

    before do
      stub_request(:any, /^#{registry_url}/).to_rack(AuthorizedFakePrefixedConfluentSchemaRegistryServer)
      AuthorizedFakePrefixedConfluentSchemaRegistryServer.clear
    end

    it_behaves_like "encoding and decoding with the schema from schema store"
    it_behaves_like 'encoding and decoding with the schema from registry'
    it_behaves_like 'encoding and decoding with the schema_id from registry'
  end

  context 'with a connect timeout' do
    let(:avro) {
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        schemas_path: "spec/schemas",
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass,
        connect_timeout: 10
      )
    }

    it_behaves_like "encoding and decoding with the schema from schema store"
    it_behaves_like 'encoding and decoding with the schema from registry'
    it_behaves_like 'encoding and decoding with the schema_id from registry'

    it 'passes the connect timeout setting to Excon' do
      expect(Excon).to receive(:new).with(anything, hash_including(connect_timeout: 10)).and_call_original
      avro
    end
  end

  context 'with a connect timeout' do
    let(:avro) {
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        schemas_path: "spec/schemas",
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass,
        retry_limit: 5
      )
    }

    it_behaves_like "encoding and decoding with the schema from schema store"
    it_behaves_like 'encoding and decoding with the schema from registry'
    it_behaves_like 'encoding and decoding with the schema_id from registry'

    it 'passes the connect timeout setting to Excon' do
      expect(Excon).to receive(:new).with(anything, hash_including(retry_limit: 5)).and_call_original
      avro
    end
  end

  context 'with a proxy' do
    let(:proxy_url) { 'http://proxy.example.com' }
    let(:avro) {
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        schemas_path: "spec/schemas",
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass,
        proxy: proxy_url
      )
    }

    it_behaves_like "encoding and decoding with the schema from schema store"
    it_behaves_like 'encoding and decoding with the schema from registry'
    it_behaves_like 'encoding and decoding with the schema_id from registry'

    it 'passes the proxy setting to Excon' do
      expect(Excon).to receive(:new).with(anything, hash_including(proxy: proxy_url)).and_call_original
      avro
    end
  end


  context 'with a custom domain name resolver' do
    let(:resolv_resolver) { Resolv.new([Resolv::Hosts.new, Resolv::DNS.new(nameserver: ['127.0.0.1', '127.0.0.1'])]) }
    let(:avro) {
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        schemas_path: "spec/schemas",
        logger: logger,
        client_cert: client_cert,
        client_key: client_key,
        client_key_pass: client_key_pass,
        resolv_resolver: resolv_resolver
      )
    }

    it_behaves_like "encoding and decoding with the schema from schema store"
    it_behaves_like 'encoding and decoding with the schema from registry'
    it_behaves_like 'encoding and decoding with the schema_id from registry'

    it 'passes the domain name resolver setting to Excon' do
      expect(Excon).to receive(:new).with(anything, hash_including(resolv_resolver: resolv_resolver)).and_call_original
      avro
    end
  end
end
