require 'webmock/rspec'
require 'avro_turf/cached_confluent_schema_registry'
require 'avro_turf/test/fake_confluent_schema_registry_server'

describe AvroTurf::CachedConfluentSchemaRegistry do
  let(:upstream) { instance_double(AvroTurf::ConfluentSchemaRegistry) }
  let(:registry) { described_class.new(upstream) }
  let(:id) { rand(999) }
  let(:schema) do
    {
      type: "record",
      name: "person",
      fields: [{ name: "name", type: "string" }]
    }.to_json
  end

  describe "#fetch" do
    it "caches the result of fetch" do
      allow(upstream).to receive(:fetch).with(id).and_return(schema)
      registry.fetch(id)
      expect(registry.fetch(id)).to eq(schema)
      expect(upstream).to have_received(:fetch).exactly(1).times
    end
  end

  describe "#register" do
    let(:subject_name) { "a_subject" }

    it "caches the result of register" do
      allow(upstream).to receive(:register).with(subject_name, schema).and_return(id)
      registry.register(subject_name, schema)
      expect(registry.register(subject_name, schema)).to eq(id)
      expect(upstream).to have_received(:register).exactly(1).times
    end
  end

  it_behaves_like "a confluent schema registry client" do
    let(:upstream) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }
    let(:registry) { described_class.new(upstream) }
  end
end
