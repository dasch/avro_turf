# frozen_string_literal: true

require "avro_turf/schema_store"

class AvroTurf
  # A schema store that allows you to add or remove schemas, and to access
  # them externally.
  #
  # Only the top-level schema is cached. It is important to not register
  # sub-schema as other schemas may define the same sub-schema and
  # the Avro gem will raise an Avro::SchemaParseError when parsing another
  # schema with a subschema with the same name as one encounted previously:
  # <Avro::SchemaParseError: The name "foo.bar" is already in use.>
  #
  # Essentially, the only schemas that should be resolvable in @schemas
  # are those that have their own .avsc files on disk.
  #
  # See https://github.com/dasch/avro_turf/pull/111
  # and the implementation in AvroTurf::SchemaStore#load_schema!
  class MutableSchemaStore < SchemaStore
    attr_accessor :schemas

    # @param schema_hash [Hash]
    def add_schema(schema_hash)
      name = schema_hash["name"]
      namespace = schema_hash["namespace"]
      full_name = Avro::Name.make_fullname(name, namespace)
      return if @schemas.key?(full_name)

      # We pass in copy of @schemas which Avro can freely modify
      # and register the sub-schema. It doesn't matter because
      # we will discard it.
      schema = Avro::Schema.real_parse(schema_hash, @schemas.dup)
      @schemas[full_name] = schema

      schema
    end
  end
end
