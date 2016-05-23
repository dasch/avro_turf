require 'webmock/rspec'
require 'avro_turf/messaging'
require_relative 'fake_schema_registry_server'

describe AvroTurf::Messaging do
  let(:registry_url) { "http://registry.example.com" }
  let(:logger) { Logger.new(StringIO.new) }

  describe 'local' do
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

    context "when active_support/core_ext is present" do
      let(:avro) do
        super().tap do |messaging|
          # Simulate the presence of active_support/core_ext by monkey patching
          # the schema store to monkey patch #to_json on the returned schema.
          schema_store = messaging.instance_variable_get(:@schema_store)
          def schema_store.find(*)
            super.extend(Module.new do
              # Replace to_json on the returned schema with an implementation
              # that returns something similar to active_support/core_ext/json
              def to_json(*args)
                instance_variables.each_with_object(Hash.new) do |ivar, result|
                  result[ivar.to_s.sub('@','')] = instance_variable_get(ivar)
                end.to_json(*args)
              end
            end)
          end
        end
      end

      it "encodes and decodes messages" do
        message = { "full_name" => "John Doe" }
        data = avro.encode(message, schema_name: "person")
        expect(avro.decode(data)).to eq message
      end
    end
  end # local

  describe 'register from rest proxy' do
    let(:avro) do
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        logger: logger,
        register: false
      )
    end

    let :schema do
      {
        'subject' => 'person',
        'version' => 1,
        'id' => 3,
        'schema' => {
          type: 'record',
          name: 'person',
          namespace: 'namespace',
          fields: [{ name: 'full_name', type: 'string' }]
        }.to_json
      }
    end

    before :each do
      stub_request(:get, "#{registry_url}/subjects/person/versions/latest")
        .to_return(status: 200, body: schema.to_json)
      # stub_request(:get, "#{registry_url}/schemas/ids/#{schema[:id]}")
        # .to_return(status: 200, body: schema.to_json)
    end

    it 'loads from schema resgitry' do
      message = { "full_name" => "John Doe" }
      expect(avro).to receive(:register_found_schema).with(schema, 'person')
        .and_return([Avro::Schema.parse(schema['schema']), schema['id']])
      data = avro.encode(message, schema_name: "person")

      sch = Avro::Schema.parse(schema['schema'])
      schema_store = avro.instance_variable_get(:@schema_store)
      registry = avro.instance_variable_get(:@registry)
      schema_store.store!(sch.fullname, sch)
      registry.store!(sch.fullname, sch, schema['id'])
      expect(avro.decode(data, schema_name: 'person', namespace: 'namespace')).to eq message
    end

    it 'already exists in cache' do
      # Register the schema
      sch, id = avro.send(:register_found_schema, schema, 'person')

      message = { "full_name" => "John Doe" }
      expect(avro).to_not receive(:register_found_schema).with(schema, 'person')
      data = avro.encode(message, schema_name: "person", namespace: 'namespace')
      expect(avro.decode(data, schema_name: 'person', namespace: 'namespace')).to eq message
    end
  end

  describe 'register_found_schema' do
    let(:avro) do
      AvroTurf::Messaging.new(
        registry_url: registry_url,
        logger: logger,
        register: false
      )
    end

    let :schema do
      {
        'subject' => 'person',
        'version' => 1,
        'id' => 3,
        'schema' => {
          type: 'record',
          name: 'person',
          namespace: 'namespace',
          fields: [{ name: 'full_name', type: 'string' }]
        }.to_json
      }
    end

    it 'fails without a schema' do
      expect do
        avro.send(:register_found_schema, nil, 'person')
      end.to raise_error RuntimeError
    end

    it 'stores the schema' do
      sch, id = avro.send(:register_found_schema, schema, 'person')
      expect(sch).to eq Avro::Schema.parse(schema['schema'])
      expect(id).to eq schema['id']
      store = avro.instance_variable_get(:@schema_store)
      registry = avro.instance_variable_get(:@registry)
      schemas = { sch.fullname => sch }
      schemas_by_ids = { schema['id'] => sch.to_s }
      ids_by_schema = { sch.fullname + schema['schema'] => schema['id'] }

      expect(store.instance_variable_get(:@schemas)).to eq schemas
      expect(registry.instance_variable_get(:@schemas_by_id)).to eq schemas_by_ids
      expect(registry.instance_variable_get(:@ids_by_schema)).to eq ids_by_schema
    end
  end
end
