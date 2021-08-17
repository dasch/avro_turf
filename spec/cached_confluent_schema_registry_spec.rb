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
      # multiple calls return same result, with only one upstream call
      allow(upstream).to receive(:fetch).with(id).and_return(schema)
      expect(registry.fetch(id)).to eq(schema)
      expect(registry.fetch(id)).to eq(schema)
      expect(upstream).to have_received(:fetch).exactly(1).times
    end
  end

  describe "#register" do
    let(:subject_name) { "a_subject" }

    it "caches the result of register" do
      # multiple calls return same result, with only one upstream call
      allow(upstream).to receive(:register).with(subject_name, schema, []).and_return(id)
      expect(registry.register(subject_name, schema)).to eq(id)
      expect(registry.register(subject_name, schema)).to eq(id)
      expect(upstream).to have_received(:register).exactly(1).times
    end
  end

  describe '#subject_version' do
    let(:subject_name) { 'a_subject' }
    let(:version) { 1 }
    let(:schema_with_meta) do
      {
        subject: subject_name,
        id: 1,
        version: 1,
        references: [],
        schema: schema
      }
    end

    it 'caches the result of subject_version' do
      allow(upstream).to receive(:subject_version).with(subject_name, version).and_return(schema_with_meta)
      registry.subject_version(subject_name, version)
      registry.subject_version(subject_name, version)
      expect(upstream).to have_received(:subject_version).exactly(1).times
    end
  end

  it_behaves_like "a confluent schema registry client" do
    let(:upstream) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }
    let(:registry) { described_class.new(upstream) }
  end
end
