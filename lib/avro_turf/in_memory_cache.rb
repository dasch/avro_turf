# A cache for the CachedConfluentSchemaRegistry.
# Simply stores the schemas and ids in in-memory hashes.
class AvroTurf::InMemoryCache

  def initialize
    @schemas_by_id = {}
    @ids_by_schema = {}
    @schema_by_subject_version = {}
  end

  def lookup_by_id(id)
    @schemas_by_id[id]
  end

  def store_by_id(id, schema)
    @schemas_by_id[id] = schema
  end

  def lookup_by_schema(subject, schema)
    key = [subject, schema]
    @ids_by_schema[key]
  end

  def store_by_schema(subject, schema, id)
    key = [subject, schema]
    @ids_by_schema[key] = id
  end

  def lookup_by_version(subject, version)
    key = "#{subject}#{version}"
    @schema_by_subject_version[key]
  end

  def store_by_version(subject, version, schema)
    key = "#{subject}#{version}"
    @schema_by_subject_version[key] = schema
  end
end
