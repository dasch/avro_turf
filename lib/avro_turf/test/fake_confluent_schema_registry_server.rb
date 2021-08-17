require 'sinatra/base'

class FakeConfluentSchemaRegistryServer < Sinatra::Base
  SUBJECTS = Hash.new { Array.new }
  REFERENCES = {}
  SCHEMAS = []
  CONFIGS = Hash.new
  SUBJECT_NOT_FOUND = { error_code: 40401, message: 'Subject not found' }.to_json.freeze
  VERSION_NOT_FOUND = { error_code: 40402, message: 'Version not found' }.to_json.freeze
  SCHEMA_NOT_FOUND = { error_code: 40403, message: 'Schema not found' }.to_json.freeze
  DEFAULT_GLOBAL_CONFIG = { 'compatibility' => 'BACKWARD'.freeze }.freeze

  @global_config = DEFAULT_GLOBAL_CONFIG.dup

  class << self
    attr_reader :global_config
  end

  helpers do
    def parse_schema
      request.body.rewind
      JSON.parse(request.body.read).fetch("schema")
    end

    def parse_body
      request.body.rewind
      JSON.parse(request.body.read)
    end

    def parse_config
      request.body.rewind
      JSON.parse(request.body.read)
    end

    def global_config
      self.class.global_config
    end
  end

  post "/subjects/:subject/versions" do
    body = parse_body
    subject = params[:subject]
    schema = body.fetch("schema")
    REFERENCES[subject] = body.fetch("references", [])
    schemas_for_subject = SUBJECTS[subject].map { |idx| SCHEMAS[idx] }.compact

    if schemas_for_subject.include?(schema)
      schema_id = SCHEMAS.index(schema)
    else
      SCHEMAS << schema
      schema_id = SCHEMAS.size - 1
      SUBJECTS[subject] <<= schema_id
    end

    { id: schema_id }.to_json
  end

  get "/schemas/ids/:schema_id" do
    schema = SCHEMAS.at(params[:schema_id].to_i)
    halt(404, SCHEMA_NOT_FOUND) unless schema
    { schema: schema }.to_json
  end

  get "/schemas/ids/:schema_id/versions" do
    schema = SCHEMAS.at(params[:schema_id].to_i)
    halt(404, SCHEMA_NOT_FOUND) unless schema
    subject = JSON.parse(schema)["name"]
    [{ subject: subject, version: 1 }].to_json
  end

  get "/subjects" do
    SUBJECTS.keys.to_json
  end

  get "/subjects/:subject/versions" do
    schema_ids = SUBJECTS[params[:subject]]
    halt(404, SUBJECT_NOT_FOUND) if schema_ids.empty?
    (1..schema_ids.size).to_a.to_json
  end

  get "/subjects/:subject/versions/:version" do
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
      schema: schema,
      references: REFERENCES.fetch(params[:subject], [])
    }.to_json
  end

  post "/subjects/:subject" do
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

  post "/compatibility/subjects/:subject/versions/:version" do
    # The ruby avro gem does not yet include a compatibility check between schemas.
    # See https://github.com/apache/avro/pull/170
    raise NotImplementedError
  end

  get "/config" do
    global_config.to_json
  end

  put "/config" do
    global_config.merge!(parse_config).to_json
  end

  get "/config/:subject" do
    CONFIGS.fetch(params[:subject], global_config).to_json
  end

  put "/config/:subject" do
    config = parse_config
    subject = params[:subject]
    CONFIGS.fetch(subject) do
      CONFIGS[subject] = {}
    end.merge!(config).to_json
  end

  def self.clear
    SUBJECTS.clear
    SCHEMAS.clear
    CONFIGS.clear
    @global_config = DEFAULT_GLOBAL_CONFIG.dup
  end
end
