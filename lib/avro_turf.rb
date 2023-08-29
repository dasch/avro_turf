begin
  require 'avro-patches'
rescue LoadError
  false
end
require 'avro_turf/version'
require 'avro'
require 'json'
require 'avro_turf/schema_store'
require 'avro_turf/core_ext'

# check for something that indicates Avro v1.9.0 or later
unless defined?(::Avro::LogicalTypes)
  require 'avro_turf/schema_to_avro_patch'
end

class AvroTurf
  class Error < StandardError; end

  class SchemaError < Error; end

  class SchemaNotFoundError < Error; end

  DEFAULT_SCHEMAS_PATH = "./schemas"

  # Create a new AvroTurf instance with the specified configuration.
  #
  # schemas_path - The String path to the root directory containing Avro schemas (default: "./schemas").
  # schema_store - A schema store object that responds to #find(schema_name, namespace).
  # namespace    - The String namespace that should be used to qualify schema names (optional).
  # codec        - The String name of a codec that should be used to compress messages (optional).
  #
  # Currently, the only valid codec name is `deflate`.
  def initialize(schemas_path: nil, schema_store: nil, namespace: nil, codec: nil)
    @namespace = namespace
    @schema_store = schema_store ||
      SchemaStore.new(path: schemas_path || DEFAULT_SCHEMAS_PATH)
    @codec = codec
  end

  # Encodes data to Avro using the specified schema.
  #
  # data        - The data that should be encoded.
  # schema_name - The name of a schema in the `schemas_path`.
  # validate    - The boolean for performing complete data validation before
  #               encoding it, Avro::SchemaValidator::ValidationError with
  #               a descriptive message will be raised in case of invalid message.
  #
  # Returns a String containing the encoded data.
  def encode(data, schema_name: nil, namespace: @namespace, validate: false)
    stream = StringIO.new

    encode_to_stream(data, stream: stream, schema_name: schema_name, namespace: namespace, validate: validate)

    stream.string
  end

  # Encodes data to Avro using the specified schema and writes it to the
  # specified stream.
  #
  # data        - The data that should be encoded.
  # schema_name - The name of a schema in the `schemas_path`.
  # stream      - An IO object that the encoded data should be written to (optional).
  # validate    - The boolean for performing complete data validation before
  #               encoding it, Avro::SchemaValidator::ValidationError with
  #               a descriptive message will be raised in case of invalid message.
  # validate_options - Hash for the Avro::SchemaValidator, default
  #               {recursive: true, encoded: false, fail_on_extra_fields: true}
  #
  # Returns nothing.
  def encode_to_stream(data, schema_name: nil, stream: nil, namespace: @namespace,
                       validate: false,
                       validate_options: { recursive: true,
                                           encoded: false,
                                           fail_on_extra_fields: true })
    schema = @schema_store.find(schema_name, namespace)
    writer = Avro::IO::DatumWriter.new(schema)

    if validate
      Avro::SchemaValidator.validate!(schema, data, **validate_options)
    end

    dw = Avro::DataFile::Writer.new(stream, writer, schema, @codec)
    dw << data.as_avro
    dw.close
  end

  # Decodes Avro data.
  #
  # encoded_data - A String containing Avro-encoded data.
  # schema_name  - The String name of the schema that should be used to read
  #                the data. If nil, the writer schema will be used.
  # namespace    - The namespace of the Avro schema used to decode the data.
  #
  # Returns whatever is encoded in the data.
  def decode_first(encoded_data, schema_name: nil, namespace: @namespace)
    stream = StringIO.new(encoded_data)
    decode_stream(stream, schema_name: schema_name, namespace: namespace)
  end

  alias decode decode_first

  # Returns all entries encoded in the data.
  def decode_all(encoded_data, schema_name: nil, namespace: @namespace)
    stream = StringIO.new(encoded_data)
    decode_all_from_stream(stream, schema_name: schema_name, namespace: namespace)
  end

  # Decodes the first entry from an IO stream containing Avro data.
  #
  # stream       - An IO object containing Avro data.
  # schema_name  - The String name of the schema that should be used to read
  #                the data. If nil, the writer schema will be used.
  # namespace    - The namespace of the Avro schema used to decode the data.
  #
  # Returns first entry encoded in the stream.
  def decode_first_from_stream(stream, schema_name: nil, namespace: @namespace)
    data = decode_all_from_stream(stream, schema_name: schema_name, namespace: namespace)
    data.first
  end

  alias decode_stream decode_first_from_stream

  # Returns all entries encoded in the stream.
  def decode_all_from_stream(stream, schema_name: nil, namespace: @namespace)
    schema = schema_name && @schema_store.find(schema_name, namespace)
    reader = Avro::IO::DatumReader.new(nil, schema)
    Avro::DataFile::Reader.new(stream, reader)
  end

  # Validates data against an Avro schema.
  #
  # data        - The data that should be validated.
  # schema    - The String name of the schema that should be used to validate
  #             the data.
  # namespace - The namespace of the Avro schema (optional).
  #
  # Returns true if the data is valid, false otherwise.
  def valid?(data, schema_name: nil, namespace: @namespace, validate_options: {})
    schema = schema_name && @schema_store.find(schema_name, namespace)
    Avro::Schema.validate(schema, data.as_avro, **validate_options)
  end

  # Loads all schema definition files in the `schemas_dir`.
  def load_schemas!
    @schema_store.load_schemas!
  end
end
