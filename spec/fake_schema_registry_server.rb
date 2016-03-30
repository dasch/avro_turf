require 'sinatra/base'

class FakeSchemaRegistryServer < Sinatra::Base
  SCHEMAS = []

  helpers do
    def validate_schema(schema)
      Avro::Schema.parse(schema)
    end
  end

  post "/subjects/:subject/versions" do
    request.body.rewind
    schema = JSON.parse(request.body.read).fetch("schema")

    validate_schema(schema)
    SCHEMAS << schema
    schema_id = SCHEMAS.size - 1

    { id: schema_id }.to_json
  end

  get "/schemas/ids/:schema_id" do
    schema = SCHEMAS.at(params[:schema_id].to_i)

    { schema: schema }.to_json
  end

  def self.clear
    SCHEMAS.clear
  end
end
