require 'sinatra/base'

class FakeConfluentSchemaRegistryServer < Sinatra::Base
  QUALIFIED_SUBJECT = /
      :(?<context>\.[^:]*)
      :(?<subject>.*)
    /x
  DEFAULT_CONTEXT = '.'
  SUBJECTS = Hash.new { |hash, key| hash[key] = Hash.new { Array.new } }
  SCHEMAS = Hash.new { |hash, key| hash[key] = Array.new }
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
      JSON.parse(request.body.read).fetch("schema").tap do |schema|
        Avro::Schema.parse(schema)
      end
    end

    def parse_config
      request.body.rewind
      JSON.parse(request.body.read)
    end

    def global_config
      self.class.global_config
    end
  end

  post "/subjects/:qualified_subject/versions" do
    schema = parse_schema
    context, subject = parse_qualified_subject(params[:qualified_subject])
    schema_id = SCHEMAS[context].index(schema)
    if schema_id.nil?
      SCHEMAS[context] << schema
      schema_id = SCHEMAS[context].size - 1
    end

    unless SUBJECTS[context][subject].include?(schema_id)
      SUBJECTS[context][subject] = SUBJECTS[context][subject] << schema_id
    end

    { id: schema_id }.to_json
  end

  get "/schemas/ids/:schema_id/versions" do
    schema_id = params[:schema_id].to_i
    context, _subject = parse_qualified_subject(params[:subject])
    schema = SCHEMAS[context].at(schema_id)
    halt(404, SCHEMA_NOT_FOUND) unless schema

    related_subjects = SUBJECTS[context].select {|_, vs| vs.include? schema_id }

    related_subjects.map do |subject, versions|
      {
        subject: subject,
        version: versions.find_index(schema_id) + 1
      }
    end.to_json
  end

  get "/schemas/ids/:schema_id" do
    context, _subject = parse_qualified_subject(params[:subject])
    schema = SCHEMAS[context].at(params[:schema_id].to_i)
    halt(404, SCHEMA_NOT_FOUND) unless schema
    { schema: schema }.to_json
  end

  get "/subjects" do
    subject_names = SUBJECTS.reduce([]) do |acc, args|
      context, subjects = args
      subjects.keys.each { |subject| acc << (context == '.' ? subject : ":#{context}:#{subject}") }
      acc
    end
    subject_names.to_json
  end

  get "/subjects/:qualified_subject/versions" do
    context, subject = parse_qualified_subject(params[:qualified_subject])
    schema_ids = SUBJECTS[context][subject]
    halt(404, SUBJECT_NOT_FOUND) if schema_ids.empty?
    (1..schema_ids.size).to_a.to_json
  end

  get "/subjects/:qualified_subject/versions/:version" do
    context, subject = parse_qualified_subject(params[:qualified_subject])
    schema_ids = SUBJECTS[context][subject]
    halt(404, SUBJECT_NOT_FOUND) if schema_ids.empty?

    schema_id = if params[:version] == 'latest'
                  schema_ids.last
                else
                  schema_ids.at(Integer(params[:version]) - 1)
                end
    halt(404, VERSION_NOT_FOUND) unless schema_id

    schema = SCHEMAS[context].at(schema_id)

    {
      name: params[:qualified_subject],
      version: schema_ids.index(schema_id) + 1,
      id: schema_id,
      schema: schema
    }.to_json
  end

  post "/subjects/:qualified_subject" do
    schema = parse_schema

    # Note: this does not actually handle the same schema registered under
    # multiple subjects
    context, subject = parse_qualified_subject(params[:qualified_subject])
    schema_id = SCHEMAS[context].index(schema)

    halt(404, SCHEMA_NOT_FOUND) unless schema_id

    {
      subject: params[:qualified_subject],
      id: schema_id,
      version: SUBJECTS[context][subject].index(schema_id) + 1,
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

  private

  def parse_qualified_subject(qualified_subject)
    match = QUALIFIED_SUBJECT.match(qualified_subject)
    if !match.nil?
      match.named_captures.values_at('context', 'subject')
    else
      [ DEFAULT_CONTEXT, qualified_subject]
    end
  end
end
