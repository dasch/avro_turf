require 'avro_turf/schema_registry'

# Caches registrations and lookups to the schema registry in memory.
class AvroTurf::CachedSchemaRegistry

  def initialize(upstream, register: true)
    @upstream = upstream
    @schemas_by_id = {}
    @ids_by_schema = {}
    @register = register
  end

  # Delegate the following methods to the upstream
  %i(subjects subject_versions subject_version check).each do |name|
    define_method(name) do |*args|
      instance_variable_get(:@upstream).send(name, *args)
    end
  end

  def fetch(id)
    @schemas_by_id[id] ||= @upstream.fetch(id)
  end

  def register(subject, schema)
    @ids_by_schema[subject + schema.to_s] ||= upstream_register(subject, schema)
  end

  def upstream_register(subject, schema)
    @upstream.register(subject, schema) if @register
  end

  def store!(subject, schema, id)
    @schemas_by_id[id] = schema.to_s
    @ids_by_schema[subject + schema.to_s] = id
  end
end
