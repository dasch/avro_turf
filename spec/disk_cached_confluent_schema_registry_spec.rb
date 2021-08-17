require 'webmock/rspec'
require 'avro_turf/cached_confluent_schema_registry'
require 'avro_turf/test/fake_confluent_schema_registry_server'

describe AvroTurf::CachedConfluentSchemaRegistry do
  let(:upstream) { instance_double(AvroTurf::ConfluentSchemaRegistry) }
  let(:logger_io) { StringIO.new }
  let(:cache)    { AvroTurf::DiskCache.new("spec/cache", logger: Logger.new(logger_io))}
  let(:registry) { described_class.new(upstream, cache: cache) }
  let(:id) { rand(999) }
  let(:schema) do
    {
      type: "record",
      name: "person",
      fields: [{ name: "name", type: "string" }]
    }.to_json
  end

  let(:city_id) { rand(999) }
  let(:city_schema) do
    {
      type: "record",
      name: "city",
      fields: [{ name: "name", type: "string" }]
    }.to_json
  end

  let(:subject) { 'subject' }
  let(:version) { rand(999) }
  let(:subject_version_schema) do
    {
      subject: subject,
      version: version,
      id: id,
      schema:  {
        type: "record",
        name: "city",
        fields: { name: "name", type: "string" }
      }
    }.to_json
  end

  before do
    FileUtils.mkdir_p("spec/cache")
  end

  describe "#fetch" do
    let(:cache_before) do
      {
        "#{id}" => "#{schema}"
      }
    end
    let(:cache_after) do
      {
        "#{id}" => "#{schema}",
        "#{city_id}" => "#{city_schema}"
      }
    end

    # setup the disk cache to avoid performing the upstream fetch
    before do
      store_cache("schemas_by_id.json", cache_before)
    end

    it "uses preloaded disk cache" do
      # multiple calls return same result, with zero upstream calls
      allow(upstream).to receive(:fetch).with(id).and_return(schema)
      expect(registry.fetch(id)).to eq(schema)
      expect(registry.fetch(id)).to eq(schema)
      expect(upstream).to have_received(:fetch).exactly(0).times
      expect(load_cache("schemas_by_id.json")).to eq cache_before
    end

    it "writes thru to disk cache" do
      # multiple calls return same result, with only one upstream call
      allow(upstream).to receive(:fetch).with(city_id).and_return(city_schema)
      expect(registry.fetch(city_id)).to eq(city_schema)
      expect(registry.fetch(city_id)).to eq(city_schema)
      expect(upstream).to have_received(:fetch).exactly(1).times
      expect(load_cache("schemas_by_id.json")).to eq cache_after
    end
  end

  describe "#fetch (zero length cache file)" do
    let(:cache_after) do
      {
        "#{id}" => "#{schema}"
      }
    end

    before do
      # setup the disk cache with a zero length file
      File.write(File.join("spec/cache", "schemas_by_id.json"), '')
    end

    it "skips zero length disk cache" do
      # multiple calls return same result, with only one upstream call
      allow(upstream).to receive(:fetch).with(id).and_return(schema)
      expect(registry.fetch(id)).to eq(schema)
      expect(registry.fetch(id)).to eq(schema)
      expect(upstream).to have_received(:fetch).exactly(1).times
      expect(load_cache("schemas_by_id.json")).to eq cache_after
      expect(logger_io.string).to include("zero length file at spec/cache/schemas_by_id.json")
    end
  end

  describe "#fetch (corrupt cache file)" do
    before do
      # setup the disk cache with a corrupt file (i.e. not json)
      File.write(File.join("spec/cache", "schemas_by_id.json"), 'NOTJSON')
    end

    it "raises error on corrupt cache file" do
      expect{registry.fetch(id)}.to raise_error(JSON::ParserError, /unexpected token/)
    end
  end

  describe "#register" do
    let(:subject_name) { "a_subject" }
    let(:cache_before) do
      {
        "#{subject_name}#{schema}" => id
      }
    end

    let(:city_name) { "a_city" }
    let(:cache_after) do 
      {
        "#{subject_name}#{schema}" => id,
        "#{city_name}#{city_schema}" => city_id
      }
    end

    # setup the disk cache to avoid performing the upstream register
    before do
      store_cache("ids_by_schema.json", cache_before)
    end

    it "uses preloaded disk cache" do
      # multiple calls return same result, with zero upstream calls
      allow(upstream).to receive(:register).with(subject_name, schema).and_return(id)
      expect(registry.register(subject_name, schema)).to eq(id) 
      expect(registry.register(subject_name, schema)).to eq(id)
      expect(upstream).to have_received(:register).exactly(0).times
      expect(load_cache("ids_by_schema.json")).to eq cache_before
    end

    it "writes thru to disk cache" do
      # multiple calls return same result, with only one upstream call
      allow(upstream).to receive(:register).with(city_name, city_schema, []).and_return(city_id)
      expect(registry.register(city_name, city_schema)).to eq(city_id)
      expect(registry.register(city_name, city_schema)).to eq(city_id)
      expect(upstream).to have_received(:register).exactly(1).times
      expect(load_cache("ids_by_schema.json")).to eq cache_after
    end
  end

  describe "#register (zero length cache file)" do
    let(:subject_name) { "a_subject" }
    let(:cache_after) do
      {
        "#{subject_name}#{schema}" => id
      }
    end

    before do
      # setup the disk cache with a zero length file
      File.write(File.join("spec/cache", "ids_by_schema.json"), '')
    end

    it "skips zero length disk cache" do
      # multiple calls return same result, with only one upstream call
      allow(upstream).to receive(:register).with(subject_name, schema, []).and_return(id)
      expect(registry.register(subject_name, schema)).to eq(id)
      expect(registry.register(subject_name, schema)).to eq(id)
      expect(upstream).to have_received(:register).exactly(1).times
      expect(load_cache("ids_by_schema.json")).to eq cache_after
      expect(logger_io.string).to include("zero length file at spec/cache/ids_by_schema.json")
    end
  end

  describe "#register (corrupt cache file)" do
    before do
      # setup the disk cache with a corrupt file (i.e. not json)
      File.write(File.join("spec/cache", "ids_by_schema.json"), 'NOTJSON')
    end

    it "raises error on corrupt cache file" do
      expect{registry.register(subject_name, schema)}.to raise_error(JSON::ParserError, /unexpected token/)
    end
  end

  describe "#subject_version" do
    it "writes thru to disk cache" do
      # multiple calls return same result, with zero upstream calls
      allow(upstream).to receive(:subject_version).with(subject, version).and_return(subject_version_schema)
      expect(File).not_to exist("./spec/cache/schemas_by_subject_version.json")

      expect(registry.subject_version(subject, version)).to eq(subject_version_schema)

      json = JSON.parse(File.read("./spec/cache/schemas_by_subject_version.json"))["#{subject}#{version}"]
      expect(json).to eq(subject_version_schema)

      expect(registry.subject_version(subject, version)).to eq(subject_version_schema)
      expect(upstream).to have_received(:subject_version).exactly(1).times
    end

    it "reads from disk cache and populates mem cache" do
      allow(upstream).to receive(:subject_version).with(subject, version).and_return(subject_version_schema)
      key = "#{subject}#{version}"
      hash = {key => subject_version_schema}
      cache.send(:write_to_disk_cache, "./spec/cache/schemas_by_subject_version.json", hash)

      cached_schema = cache.instance_variable_get(:@schemas_by_subject_version)
      expect(cached_schema).to eq({})

      expect(registry.subject_version(subject, version)).to eq(subject_version_schema)
      expect(upstream).to have_received(:subject_version).exactly(0).times

      cached_schema = cache.instance_variable_get(:@schemas_by_subject_version)
      expect(cached_schema).to eq({key => subject_version_schema})
    end
  end

  it_behaves_like "a confluent schema registry client" do
    let(:upstream) { AvroTurf::ConfluentSchemaRegistry.new(registry_url, logger: logger) }
    let(:registry) { described_class.new(upstream) }
  end
end
