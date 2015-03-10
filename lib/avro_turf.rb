require 'avro_turf/version'
require 'avro'
require 'json'
require 'avro_turf/schema_store'

class AvroTurf
  class Error < StandardError; end
  class SchemaError < Error; end
  class SchemaNotFoundError < Error; end

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
    writer = Avro::IO::DatumWriter.new(schema)

    dw = Avro::DataFile::Writer.new(stream, writer, schema)
    dw << data
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
    reader = Avro::IO::DatumReader.new(nil, schema)
    dr = Avro::DataFile::Reader.new(stream, reader)
    dr.first
  end
end
