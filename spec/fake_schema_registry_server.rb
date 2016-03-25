require 'sinatra/base'

class FakeSchemaRegistryServer < Sinatra::Base
  SUBJECTS = Hash.new { Array.new }
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
    SUBJECTS[params[:subject]] = SUBJECTS[params[:subject]] << schema_id
    { id: schema_id }.to_json
  end

  get "/schemas/ids/:schema_id" do
    schema = SCHEMAS.at(params[:schema_id].to_i)

    { schema: schema }.to_json
  end

  get "/subjects" do
    SUBJECTS.keys.to_json
  end

  get "/subjects/:subject/versions" do
    schema_ids = SUBJECTS[params[:subject]]
    (1..schema_ids.size).to_a.to_json
  end

  get "/subjects/:subject/versions/:version" do
    schema_ids = SUBJECTS[params[:subject]]

    schema_id = if params[:version] == 'latest'
                  schema_ids.last
                else
                  schema_ids.at(Integer(params[:version]) - 1)
                end

    schema = SCHEMAS.at(schema_id)

    {
      name: params[:subject],
      version: schema_ids.index(schema_id) + 1,
      schema: schema
    }.to_json
  end

  post "/subjects/:subject" do
    request.body.rewind
    schema = JSON.parse(request.body.read).fetch("schema")

    # Note: this does not actually handle the same schema registered under
    # multiple subjects
    schema_id = SCHEMAS.index(schema)

    if schema_id
      {
        subject: params[:subject],
        id: schema_id,
        version: SUBJECTS[params[:subject]].index(schema_id) + 1,
        schema: schema
      }
    else
      {
        error_code: 40403,
        message: 'Schema not found'
      }
    end.to_json
  end

  def self.clear
    SUBJECTS.clear
    SCHEMAS.clear
  end
end
