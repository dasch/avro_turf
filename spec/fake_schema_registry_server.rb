require 'sinatra/base'

class FakeSchemaRegistryServer < Sinatra::Base
  SCHEMAS = []

  post "/subjects/:subject/versions" do
    request.body.rewind
    schema = JSON.parse(request.body.read).fetch("schema")

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
