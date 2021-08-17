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
  # cache     - Optional user provided Cache object that responds to all methods in the AvroTurf::InMemoryCache interface.
  def initialize(upstream, cache: nil)
    @upstream = upstream
    @cache = cache || AvroTurf::InMemoryCache.new()
  end

  # Delegate the following methods to the upstream
  %i(subjects subject_versions check compatible?
     global_config update_global_config subject_config update_subject_config).each do |name|
    define_method(name) do |*args|
      instance_variable_get(:@upstream).send(name, *args)
    end
  end

  def fetch(id)
    @cache.lookup_by_id(id) || @cache.store_by_id(id, @upstream.fetch(id))
  end

  def fetch_subject_version(id)
    cache_id = "#{id}-sv"
    @cache.lookup_by_id(cache_id) || @cache.store_by_id(cache_id, @upstream.fetch_subject_version(id))
  end

  def register(subject, schema, refs = [])
    @cache.lookup_by_schema(subject, schema) || @cache.store_by_schema(subject, schema, @upstream.register(subject, schema, refs))
  end

  def subject_version(subject, version = 'latest')
    return @upstream.subject_version(subject, version) if version == 'latest'
    
    @cache.lookup_by_version(subject, version) ||
      @cache.store_by_version(subject, version, @upstream.subject_version(subject, version))
  end
end
