require 'logger'
require 'avro_turf'
require 'avro_turf/schema_store'
require 'avro_turf/confluent_schema_registry'
require 'avro_turf/cached_confluent_schema_registry'

# For back-compatibility require the aliases along with the Messaging API.
# These names are deprecated and will be removed in a future release.
require 'avro_turf/schema_registry'
require 'avro_turf/cached_schema_registry'

class AvroTurf
  class IncompatibleSchemaError < StandardError; end

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

    class DecodedMessage
      attr_reader :schema_id
      attr_reader :writer_schema
      attr_reader :reader_schema
      attr_reader :message

      def initialize(schema_id, writer_schema, reader_schema, message)
        @schema_id = schema_id
        @writer_schema = writer_schema
        @reader_schema = reader_schema
        @message = message
      end
    end

    # Instantiate a new Messaging instance with the given configuration.
    #
    # registry             - A schema registry object that responds to all methods in the
    #                        AvroTurf::ConfluentSchemaRegistry interface.
    # registry_url         - The String URL of the schema registry that should be used.
    # schema_context       - Schema registry context name (optional)
    # schema_store         - A schema store object that responds to #find(schema_name, namespace).
    # schemas_path         - The String file system path where local schemas are stored.
    # namespace            - The String default schema namespace.
    # registry_path_prefix - The String URL path prefix used to namespace schema registry requests (optional).
    # logger               - The Logger that should be used to log information (optional).
    # proxy                - Forward the request via  proxy (optional).
    # user                 - User for basic auth (optional).
    # password             - Password for basic auth (optional).
    # ssl_ca_file          - Name of file containing CA certificate (optional).
    # client_cert          - Name of file containing client certificate (optional).
    # client_key           - Name of file containing client private key to go with client_cert (optional).
    # client_key_pass      - Password to go with client_key (optional).
    # client_cert_data     - In-memory client certificate (optional).
    # client_key_data      - In-memory client private key to go with client_cert_data (optional).
    # connect_timeout      - Timeout to use in the connection with the schema registry (optional).
    # resolv_resolver      - Custom domain name resolver (optional).
    def initialize(
      registry: nil,
      registry_url: nil,
      schema_context: nil,
      schema_store: nil,
      schemas_path: nil,
      namespace: nil,
      registry_path_prefix: nil,
      logger: nil,
      proxy: nil,
      user: nil,
      password: nil,
      ssl_ca_file: nil,
      client_cert: nil,
      client_key: nil,
      client_key_pass: nil,
      client_cert_data: nil,
      client_key_data: nil,
      connect_timeout: nil,
      resolv_resolver: nil,
      retry_limit: nil
    )
      @logger = logger || Logger.new($stderr)
      @namespace = namespace
      @schema_store = schema_store || SchemaStore.new(path: schemas_path || DEFAULT_SCHEMAS_PATH)
      @registry = registry || CachedConfluentSchemaRegistry.new(
        ConfluentSchemaRegistry.new(
          registry_url,
          schema_context: schema_context,
          logger: @logger,
          proxy: proxy,
          user: user,
          password: password,
          ssl_ca_file: ssl_ca_file,
          client_cert: client_cert,
          client_key: client_key,
          client_key_pass: client_key_pass,
          client_cert_data: client_cert_data,
          client_key_data: client_key_data,
          path_prefix: registry_path_prefix,
          connect_timeout: connect_timeout,
          resolv_resolver: resolv_resolver,
          retry_limit: retry_limit
        )
      )
      @schemas_by_id = {}
    end

    # Encodes a message using the specified schema.
    #
    # message           - The message that should be encoded. Must be compatible with
    #                     the schema.
    # schema_name       - The String name of the schema that should be used to encode
    #                     the data.
    # namespace         - The namespace of the schema (optional).
    # subject           - The subject name the schema should be registered under in
    #                     the schema registry (optional).
    # version           - The integer version of the schema that should be used to decode
    #                     the data. Must match the schema used when encoding (optional).
    # schema_id         - The integer id of the schema that should be used to encode
    #                     the data.
    # validate          - The boolean for performing complete message validation before
    #                     encoding it, Avro::SchemaValidator::ValidationError with
    #                     a descriptive message will be raised in case of invalid message.
    # register_schemas  - The boolean that indicates whether or not the schema should be
    #                     registered in case it does not exist, or if it should be fetched
    #                     from the registry without registering it (register_schemas: false).
    #
    # Returns the encoded data as a String.
    def encode(message, schema_name: nil, namespace: @namespace, subject: nil, version: nil, schema_id: nil, validate: false,
               register_schemas: true)
      schema, schema_id = if schema_id
        fetch_schema_by_id(schema_id)
      elsif subject && version
        fetch_schema(subject: subject, version: version)
      elsif schema_name && !register_schemas
        fetch_schema_by_body(subject: subject, schema_name: schema_name, namespace: namespace)
      elsif schema_name
        register_schema(subject: subject, schema_name: schema_name, namespace: namespace)
      else
        raise ArgumentError.new('Neither schema_name nor schema_id nor subject + version provided to determine the schema.')
      end

      if validate
        Avro::SchemaValidator.validate!(schema, message, recursive: true, encoded: false, fail_on_extra_fields: true)
      end

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
    rescue Excon::Error::NotFound
      if schema_id
        raise SchemaNotFoundError.new("Schema with id: #{schema_id} is not found on registry")
      else
        raise SchemaNotFoundError.new("Schema with subject: `#{subject}` version: `#{version}` is not found on registry")
      end
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
      decode_message(data, schema_name: schema_name, namespace: namespace).message
    end

    # Decodes data into the original message.
    #
    # data        - A String containing encoded data.
    # schema_name - The String name of the schema that should be used to decode
    #               the data. Must match the schema used when encoding (optional).
    # namespace   - The namespace of the schema (optional).
    #
    # Returns Struct with the next attributes:
    #   schema_id  - The integer id of schema used to encode the message
    #   message    - The decoded message
    def decode_message(data, schema_name: nil, namespace: @namespace)
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

      writers_schema = @schemas_by_id.fetch(schema_id) do
        schema_json = @registry.fetch(schema_id)
        @schemas_by_id[schema_id] = Avro::Schema.parse(schema_json)
      end

      reader = Avro::IO::DatumReader.new(writers_schema, readers_schema)
      message = reader.read(decoder)

      DecodedMessage.new(schema_id, writers_schema, readers_schema, message)
    rescue Excon::Error::NotFound
      raise SchemaNotFoundError.new("Schema with id: #{schema_id} is not found on registry")
    end

    # Providing subject and version to determine the schema,
    # which skips the auto registration of schema on the schema registry.
    # Fetch the schema from registry with the provided subject name and version.
    def fetch_schema(subject:, version: 'latest')
      schema_data = @registry.subject_version(subject, version)
      schema_id = schema_data.fetch('id')
      schema_type = schema_data['schemaType']
      if schema_type && schema_type != "AVRO"
        raise IncompatibleSchemaError, "The #{schema_type} schema for #{subject} is incompatible."
      end
      schema = Avro::Schema.parse(schema_data.fetch('schema'))
      [schema, schema_id]
    end

    # Fetch the schema from registry with the provided schema_id.
    def fetch_schema_by_id(schema_id)
      schema = @schemas_by_id.fetch(schema_id) do
        schema_json = @registry.fetch(schema_id)
        Avro::Schema.parse(schema_json)
      end
      [schema, schema_id]
    end

    def fetch_schema_by_body(schema_name:, subject: nil, namespace: nil)
      schema = @schema_store.find(schema_name, namespace)
      schema_data = @registry.check(subject || schema.fullname, schema)
      raise SchemaNotFoundError.new("Schema with structure: #{schema} not found on registry") unless schema_data

      [schema, schema_data["id"]]
    end

    # Schemas are registered under the full name of the top level Avro record
    # type, or `subject` if it's provided.
    def register_schema(schema_name:, subject: nil, namespace: nil)
      schema = @schema_store.find(schema_name, namespace)
      schema_id = @registry.register(subject || schema.fullname, schema)
      [schema, schema_id]
    end
  end
end
