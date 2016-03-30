require 'logger'
require 'avro_turf'
require 'avro_turf/schema_store'
require 'avro_turf/schema_registry'
require 'avro_turf/cached_schema_registry'

class AvroTurf

  # Provides a way to encode and decode messages without having to embed schemas
  # in the encoded data. Confluent's Schema Registry[1] is used to register
  # a schema when encoding a message -- the registry will issue a schema id that
  # will be included in the encoded data alongside the actual message. When
  # decoding the data, the schema id will be used to look up the writer's schema
  # from the registry.
  #
  # 1: https://github.com/confluentinc/schema-registry
  class Messaging
    MAGIC_BYTE = [0].pack("C").freeze

    # Instantiate a new Messaging instance with the given configuration.
    #
    # registry_url - The String URL of the schema registry that should be used.
    # schemas_path - The String file system path where local schemas are stored.
    # namespace    - The String default schema namespace.
    # logger       - The Logger that should be used to log information (optional).
    def initialize(registry_url: nil, schemas_path: nil, namespace: nil, logger: nil)
      @logger = logger || Logger.new($stderr)
      @namespace = namespace
      @schema_store = SchemaStore.new(path: schemas_path || DEFAULT_SCHEMAS_PATH)
      @registry = CachedSchemaRegistry.new(SchemaRegistry.new(registry_url, logger: @logger))
    end

    # Encodes a message using the specified schema.
    #
    # message     - The message that should be encoded. Must be compatible with
    #               the schema.
    # schema_name - The String name of the schema that should be used to encode
    #               the data.
    # namespace   - The namespace of the schema (optional).
    #
    # Returns the encoded data as a String.
    def encode(message, schema_name: nil, namespace: @namespace)
      schema = @schema_store.find(schema_name, namespace)

      # Schemas are registered under the full name of the top level Avro record
      # type.
      schema_id = @registry.register(schema.fullname, schema.to_s)

      stream = StringIO.new
      writer = Avro::IO::DatumWriter.new(schema)
      encoder = Avro::IO::BinaryEncoder.new(stream)

      # Always start with the magic byte.
      encoder.write(MAGIC_BYTE)

      # The schema id is encoded as a 4-byte big-endian integer.
      encoder.write([schema_id].pack("N"))

      # The actual message comes last.
      writer.write(message, encoder)

      stream.string
    end

    # Decodes data into the original message.
    #
    # data        - A String containing encoded data.
    # schema_name - The String name of the schema that should be used to decode
    #               the data. Must match the schema used when encoding (optional).
    # namespace   - The namespace of the schema (optional).
    #
    # Returns the decoded message.
    def decode(data, schema_name: nil, namespace: @namespace)
      readers_schema = schema_name && @schema_store.find(schema_name, namespace)
      stream = StringIO.new(data)
      decoder = Avro::IO::BinaryDecoder.new(stream)

      # The first byte is MAGIC!!!
      magic_byte = decoder.read(1)

      if magic_byte != MAGIC_BYTE
        raise "Expected data to begin with a magic byte, got `#{magic_byte.inspect}`"
      end

      # The schema id is a 4-byte big-endian integer.
      schema_id = decoder.read(4).unpack("N").first

      writers_schema_json = @registry.fetch(schema_id)
      writers_schema = Avro::Schema.parse(writers_schema_json)

      reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
      reader.read(decoder)
    end
  end
end
