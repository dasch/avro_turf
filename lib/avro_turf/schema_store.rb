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

      load_schema!(fullname, namespace)
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

  private

  # Loads single schema
  # Such method is not thread-safe, do not call it of from mutex synchronization routine
  def load_schema!(fullname, namespace = nil)
    *namespace, schema_name = fullname.split(".")
    schema_path = File.join(@path, *namespace, schema_name + ".avsc")
    schema = Avro::Schema.parse(File.read(schema_path))
    @schemas[fullname] = schema

    if schema.respond_to?(:fullname) && schema.fullname != fullname
      raise AvroTurf::SchemaError, "expected schema `#{schema_path}' to define type `#{fullname}'"
    end

    schema
  rescue ::Avro::SchemaParseError => e
    # This is a hack in order to figure out exactly which type was missing. The
    # Avro gem ought to provide this data directly.
    if e.to_s =~ /"([\w\.]+)" is not a schema we know about/
      load_schema!($1)

      # Re-resolve the original schema now that the dependency has been resolved.
      @schemas.delete(fullname)
      load_schema!(fullname)
    else
      raise
    end
  rescue Errno::ENOENT, Errno::ENAMETOOLONG
    raise AvroTurf::SchemaNotFoundError, "could not find Avro schema at `#{schema_path}'"
  end
end
