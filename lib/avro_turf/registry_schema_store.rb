class AvroTurf
  class RegistrySchemaStore
    def initialize(registry_url)
      @schemas = {}

      @registry = CachedConfluentSchemaRegistry.new(
        ConfluentSchemaRegistry.new(registry_url)
      )
    end

    def find(name, namespace = nil, version = "latest")
      fullname = Avro::Name.make_fullname(name, namespace)
      return @schemas.fetch(fullname) if @schemas.key?(fullname)

      schema = @schemas.fetch(fullname) do
        json_schema = JSON.parse(
          @registry.subject_version(fullname, version).fetch('schema')
        )
        Avro::Schema.real_parse(json_schema, @schemas)
      end

      schema

    rescue Excon::Error::NotFound
      raise AvroTurf::SchemaNotFoundError,
            "could not find Avro schema in the Registry: `#{fullname}` "
    end

    def load_schemas!
      @registry.subjects.each { |subject| find(subject) }
    end
  end
end
