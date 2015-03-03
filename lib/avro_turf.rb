require 'avro_turf/version'
require 'avro'

class AvroTurf
  def initialize(schemas_path:)
    @schemas_path = schemas_path
  end

  def encode(data, schema_name:)
    schema = resolve_schema(schema_name)
    writer = Avro::IO::DatumWriter.new(schema)

    io = StringIO.new
    dw = Avro::DataFile::Writer.new(io, writer, schema)
    dw << data
    dw.close

    io.string
  end

  def decode(encoded_data, schema_name:)
    io = StringIO.new(encoded_data)
    schema = resolve_schema(schema_name)
    reader = Avro::IO::DatumReader.new(nil, schema)
    dr = Avro::DataFile::Reader.new(io, reader)
    dr.first
  end

  private

  def resolve_schema(schema_name)
    schema_path = File.join(@schemas_path, schema_name + ".avsc")
    Avro::Schema.parse(File.read(schema_path))
  end
end
