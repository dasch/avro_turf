require 'sinatra/base'

class FakePrefixedConfluentSchemaRegistryServer < FakeConfluentSchemaRegistryServer
  post "/prefix/subjects/:subject/versions" do
    schema = parse_schema
    ids_for_subject = SUBJECTS[params[:subject]]

    schemas_for_subject =
      SCHEMAS.select
             .with_index { |_, i| ids_for_subject.include?(i) }

    if schemas_for_subject.include?(schema)
      schema_id = SCHEMAS.index(schema)
    else
      SCHEMAS << schema
      schema_id = SCHEMAS.size - 1
      SUBJECTS[params[:subject]] = SUBJECTS[params[:subject]] << schema_id
    end

    { id: schema_id }.to_json
  end

  get "/prefix/schemas/ids/:schema_id" do
    schema = SCHEMAS.at(params[:schema_id].to_i)
    halt(404, SCHEMA_NOT_FOUND) unless schema
    { schema: schema }.to_json
  end

  get "/prefix/subjects" do
    SUBJECTS.keys.to_json
  end

  get "/prefix/subjects/:subject/versions" do
    schema_ids = SUBJECTS[params[:subject]]
    halt(404, SUBJECT_NOT_FOUND) if schema_ids.empty?
    (1..schema_ids.size).to_a.to_json
  end

  get "/prefix/subjects/:subject/versions/:version" do
    schema_ids = SUBJECTS[params[:subject]]
    halt(404, SUBJECT_NOT_FOUND) if schema_ids.empty?

    schema_id = if params[:version] == 'latest'
                  schema_ids.last
                else
                  schema_ids.at(Integer(params[:version]) - 1)
                end
    halt(404, VERSION_NOT_FOUND) unless schema_id

    schema = SCHEMAS.at(schema_id)

    {
      name: params[:subject],
      version: schema_ids.index(schema_id) + 1,
      id: schema_id,
      schema: schema
    }.to_json
  end

  post "/prefix/subjects/:subject" do
    schema = parse_schema

    # Note: this does not actually handle the same schema registered under
    # multiple subjects
    schema_id = SCHEMAS.index(schema)

    halt(404, SCHEMA_NOT_FOUND) unless schema_id

    {
      subject: params[:subject],
      id: schema_id,
      version: SUBJECTS[params[:subject]].index(schema_id) + 1,
      schema: schema
    }.to_json
  end

  post "/prefix/compatibility/subjects/:subject/versions/:version" do
    # The ruby avro gem does not yet include a compatibility check between schemas.
    # See https://github.com/apache/avro/pull/170
    raise NotImplementedError
  end

  get "/prefix/config" do
    global_config.to_json
  end

  put "/prefix/config" do
    global_config.merge!(parse_config).to_json
  end

  get "/prefix/config/:subject" do
    CONFIGS.fetch(params[:subject], global_config).to_json
  end

  put "/prefix/config/:subject" do
    config = parse_config
    subject = params[:subject]
    CONFIGS.fetch(subject) do
      CONFIGS[subject] = {}
    end.merge!(config).to_json
  end
end
