# A cache for the CachedConfluentSchemaRegistry.
# Extends the InMemoryCache to provide a write-thru to disk for persistent cache.
class AvroTurf::DiskCache < AvroTurf::InMemoryCache

  def initialize(disk_path)
    super()

    # load the write-thru cache on startup, if it exists
    @schemas_by_id_path = File.join(disk_path, 'schemas_by_id.json')
    @schemas_by_id = JSON.parse(File.read(@schemas_by_id_path)) if File.exist?(@schemas_by_id_path)

    @ids_by_schema_path = File.join(disk_path, 'ids_by_schema.json')
    @ids_by_schema = JSON.parse(File.read(@ids_by_schema_path)) if File.exist?(@ids_by_schema_path)
  end

  # override
  # the write-thru cache (json) does not store keys in numeric format
  # so, convert id to a string for caching purposes
  def lookup_by_id(id)
    super(id.to_s)
  end

  # override to include write-thru cache after storing result from upstream
  def store_by_id(id, schema)
    # must return the value from storing the result (i.e. do not return result from file write)
    value = super(id.to_s, schema)
    File.write(@schemas_by_id_path, JSON.pretty_generate(@schemas_by_id))
    return value
  end

  # override to include write-thru cache after storing result from upstream
  def store_by_schema(subject, schema, id)
    # must return the value from storing the result (i.e. do not return result from file write)
    value = super
    File.write(@ids_by_schema_path, JSON.pretty_generate(@ids_by_schema))
    return value
  end
end
