require 'avro_turf/confluent_schema_registry'
require 'avro_turf/in_memory_cache'
require 'avro_turf/disk_cache'

# Caches registrations and lookups to the schema registry in memory.
class AvroTurf::CachedConfluentSchemaRegistry

  # Instantiate a new CachedConfluentSchemaRegistry instance with the given configuration.
  # By default, uses a provided InMemoryCache to prevent repeated calls to the upstream registry.
  #
  # upstream  - The upstream schema registry object that fully responds to all methods in the
  #             AvroTurf::ConfluentSchemaRegistry interface.
  # disk_path - Optional path on disk to use the provided DiskCache instead of the default InMemoryCache
  # cache     - Optional user provided Cache object that responds to all methods in the AvroTurf::InMemoryCache interface.
  def initialize(upstream, disk_path: nil, cache: nil)
    @upstream = upstream
    @cache = cache || create_cache(disk_path)
  end

  # Delegate the following methods to the upstream
  %i(subjects subject_versions subject_version check compatible?
     global_config update_global_config subject_config update_subject_config).each do |name|
    define_method(name) do |*args|
      instance_variable_get(:@upstream).send(name, *args)
    end
  end

  def fetch(id)
    @cache.lookup_by_id(id) || @cache.store_by_id(id, @upstream.fetch(id))
  end

  def register(subject, schema)
    @cache.lookup_by_schema(subject, schema) || @cache.store_by_schema(subject, schema, @upstream.register(subject, schema))
  end

  private 

  def create_cache(disk_path)
    disk_path ? AvroTurf::DiskCache.new(disk_path) : AvroTurf::InMemoryCache.new()
  end
end
