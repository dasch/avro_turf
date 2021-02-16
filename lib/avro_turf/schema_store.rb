class AvroTurf::SchemaStore

  def initialize(path: nil)
    @path = path or raise "Please specify a schema path"
    @schemas = Hash.new
    @mutex = Mutex.new
  end

  # Resolves and returns a schema.
  #
  # schema_name - The String name of the schema to resolve.
  #
  # Returns an Avro::Schema.
  def find(name, namespace = nil)
    fullname = Avro::Name.make_fullname(name, namespace)
    # Optimistic non-blocking read from @schemas
    # No sense to lock the resource when all the schemas already loaded
    return @schemas[fullname] if @schemas.key?(fullname)

    # Pessimistic blocking write to @schemas
    @mutex.synchronize do
      # Still need to check is the schema already loaded
      return @schemas[fullname] if @schemas.key?(fullname)

      load_schema!(fullname)
    end
  end

  # Loads all schema definition files in the `schemas_dir`.
  def load_schemas!
    pattern = [@path, "**", "*.avsc"].join("/")

    Dir.glob(pattern) do |schema_path|
      # Remove the path prefix.
      schema_path.sub!(/^\/?#{@path}\//, "")

      # Replace `/` with `.` and chop off the file extension.
      schema_name = File.basename(schema_path.tr("/", "."), ".avsc")

      # Load and cache the schema.
      find(schema_name)
    end
  end

  protected

  # Loads single schema
  # Such method is not thread-safe, do not call it of from mutex synchronization routine
  def load_schema!(fullname, local_schemas_cache = {})
    schema_path = build_schema_path(fullname)
    schema_json = JSON.parse(File.read(schema_path))

    schema = Avro::Schema.real_parse(schema_json, local_schemas_cache)

    # Don't cache the parsed schema until after its fullname is validated
    if schema.respond_to?(:fullname) && schema.fullname != fullname
      raise AvroTurf::SchemaError, "expected schema `#{schema_path}' to define type `#{fullname}'"
    end

    # Cache only this new top-level schema by its fullname. It's critical
    # not to make every sub-schema resolvable at the top level here because
    # multiple different avsc files may define the same sub-schema, and
    # if we share the @schemas cache across all parsing contexts, the Avro
    # gem will raise an Avro::SchemaParseError when parsing another avsc
    # file that contains a subschema with the same fullname as one
    # encountered previously in a different file:
    # <Avro::SchemaParseError: The name "foo.bar" is already in use.>
    # Essentially, the only schemas that should be resolvable in @schemas
    # are those that have their own .avsc files on disk.
    @schemas[fullname] = schema

    schema
  rescue ::Avro::SchemaParseError => e
    # This is a hack in order to figure out exactly which type was missing. The
    # Avro gem ought to provide this data directly.
    if e.to_s =~ /"([\w\.]+)" is not a schema we know about/
      # Try to first resolve a referenced schema from disk.
      # If this is successful, the Avro gem will have mutated the
      # local_schemas_cache, adding all the new schemas it found.
      load_schema!($1, local_schemas_cache)

      # Attempt to re-parse the original schema now that the dependency
      # has been resolved and use the now-updated local_schemas_cache to
      # pick up where we left off.
      local_schemas_cache.delete(fullname)
      local_schemas_cache.each do |schema_name, schema|
        local_schemas_cache.delete(schema_name) if schema.type_sym == :enum
      end
      load_schema!(fullname, local_schemas_cache)
    else
      raise
    end
  rescue Errno::ENOENT, Errno::ENAMETOOLONG
    raise AvroTurf::SchemaNotFoundError, "could not find Avro schema at `#{schema_path}'"
  end

  def build_schema_path(fullname)
    *namespace, schema_name = fullname.split(".")
    schema_path = File.join(@path, *namespace, schema_name + ".avsc")
  end
end
