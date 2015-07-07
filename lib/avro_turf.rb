require 'avro_turf/version'
require 'avro'
require 'json'
require 'avro_turf/schema_store'
require 'avro_turf/core_ext'

class AvroTurf
  class Error < StandardError; end
  class SchemaError < Error; end
  class SchemaNotFoundError < Error; end

  META_SCHEMA = Avro::Schema.parse('{"type": "map", "values": "bytes"}')
  META_WRITER = Avro::IO::DatumWriter.new(META_SCHEMA)
  META_READER = Avro::IO::DatumReader.new(META_SCHEMA)

  # Create a new AvroTurf instance with the specified configuration.
  #
  # schemas_path - The String path to the root directory containing Avro schemas.
  # namespace    - The String namespace that should be used to qualify schema names (optional).
  def initialize(schemas_path:, namespace: nil)
    @namespace = namespace
    @schema_store = SchemaStore.new(path: schemas_path)
  end

  # Encodes data to Avro using the specified schema.
  #
  # data        - The data that should be encoded.
  # schema_name - The name of a schema in the `schemas_path`.
  #
  # Returns a String containing the encoded data.
  def encode(data, schema_name:, namespace: @namespace)
    stream = StringIO.new

    encode_to_stream(data, stream: stream, schema_name: schema_name, namespace: namespace)

    stream.string
  end

  # Encodes data to Avro using the specified schema and writes it to the
  # specified stream.
  #
  # data        - The data that should be encoded.
  # schema_name - The name of a schema in the `schemas_path`.
  # stream      - An IO object that the encoded data should be written to (optional).
  #
  # Returns nothing.
  def encode_to_stream(data, schema_name:, stream:, namespace: @namespace)
    schema = @schema_store.find(schema_name, namespace)
    encoder = Avro::IO::BinaryEncoder.new(stream)

    meta = {
      'avro.schema' => schema.to_s
    }

    META_WRITER.write(meta, encoder)

    writer = Avro::IO::DatumWriter.new(schema)
    writer.write(data, encoder)
  end

  # Decodes Avro data.
  #
  # encoded_data - A String containing Avro-encoded data.
  # schema_name  - The String name of the schema that should be used to read
  #                the data. If nil, the writer schema will be used.
  # namespace    - The namespace of the Avro schema used to decode the data.
  #
  # Returns whatever is encoded in the data.
  def decode(encoded_data, schema_name: nil, namespace: @namespace)
    stream = StringIO.new(encoded_data)
    decode_stream(stream, schema_name: schema_name, namespace: namespace)
  end

  # Decodes Avro data from an IO stream.
  #
  # stream       - An IO object containing Avro data.
  # schema_name  - The String name of the schema that should be used to read
  #                the data. If nil, the writer schema will be used.
  # namespace    - The namespace of the Avro schema used to decode the data.
  #
  # Returns whatever is encoded in the stream.
  def decode_stream(stream, schema_name: nil, namespace: @namespace)
    schema = schema_name && @schema_store.find(schema_name, namespace)
    decoder = Avro::IO::BinaryDecoder.new(stream)
    meta = META_READER.read(decoder)
    writer_schema = Avro::Schema.parse(meta.fetch("avro.schema"))
    reader = Avro::IO::DatumReader.new(writer_schema, schema)
    reader.read(decoder)
  end

  # Loads all schema definition files in the `schemas_dir`.
  def load_schemas!
    @schema_store.load_schemas!
  end
end
