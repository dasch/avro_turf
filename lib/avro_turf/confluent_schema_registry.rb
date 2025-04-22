require 'excon'

class AvroTurf::ConfluentSchemaRegistry
  CONTENT_TYPE = "application/vnd.schemaregistry.v1+json".freeze

  def initialize(
    url,
    schema_context: nil,
    logger: Logger.new($stdout),
    proxy: nil,
    user: nil,
    password: nil,
    ssl_ca_file: nil,
    client_cert: nil,
    client_key: nil,
    client_key_pass: nil,
    client_cert_data: nil,
    client_key_data: nil,
    path_prefix: nil,
    connect_timeout: nil,
    resolv_resolver: nil
  )
    @path_prefix = path_prefix
    @schema_context_prefix = schema_context.nil? ? '' : ":.#{schema_context}:"
    @schema_context_options = schema_context.nil? ? {} : {query: {subject: @schema_context_prefix}}
    @logger = logger
    headers = Excon.defaults[:headers].merge(
      "Content-Type" => CONTENT_TYPE
    )
    headers[:proxy] = proxy unless proxy.nil?
    params = {
      headers: headers,
      user: user,
      password: password,
      ssl_ca_file: ssl_ca_file,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass,
      client_cert_data: client_cert_data,
      client_key_data: client_key_data,
      resolv_resolver: resolv_resolver
    }
    params.merge!({ connect_timeout: connect_timeout }) if connect_timeout
    @connection = Excon.new(
      url,
      params
    )
  end

  def fetch(id)
    @logger.info "Fetching schema with id #{id}"
    data = get("/schemas/ids/#{id}", idempotent: true, **@schema_context_options, )
    data.fetch("schema")
  end

  def register(subject, schema)
    data = post("/subjects/#{@schema_context_prefix}#{subject}/versions", body: { schema: schema.to_s }.to_json)

    id = data.fetch("id")

    @logger.info "Registered schema for subject `#{@schema_context_prefix}#{subject}`; id = #{id}"

    id
  end

  # List all subjects
  def subjects
    get('/subjects', idempotent: true)
  end

  # List all versions for a subject
  def subject_versions(subject)
    get("/subjects/#{@schema_context_prefix}#{subject}/versions", idempotent: true)
  end

  # Get a specific version for a subject
  def subject_version(subject, version = 'latest')
    get("/subjects/#{@schema_context_prefix}#{subject}/versions/#{version}", idempotent: true)
  end

  # Get the subject and version for a schema id
  def schema_subject_versions(schema_id)
    get("/schemas/ids/#{schema_id}/versions", idempotent: true, **@schema_context_options)
  end

  # Check if a schema exists. Returns nil if not found.
  def check(subject, schema)
    data = post("/subjects/#{@schema_context_prefix}#{subject}",
                expects: [200, 404],
                body: { schema: schema.to_s }.to_json,
                idempotent: true)
    data unless data.has_key?("error_code")
  end

  # Check if a schema is compatible with the stored version.
  # Returns:
  # - true if compatible
  # - nil if the subject or version does not exist
  # - false if incompatible
  # http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#compatibility
  def compatible?(subject, schema, version = 'latest')
    data = post("/compatibility/subjects/#{@schema_context_prefix}#{subject}/versions/#{version}",
                expects: [200, 404], body: { schema: schema.to_s }.to_json, idempotent: true)
    data.fetch('is_compatible', false) unless data.has_key?('error_code')
  end

  # Check for specific schema compatibility issues
  # Returns:
  # - nil if the subject or version does not exist
  # - a list of compatibility issues
  # https://docs.confluent.io/platform/current/schema-registry/develop/api.html#sr-api-compatibility
  def compatibility_issues(subject, schema, version = 'latest')
    data = post("/compatibility/subjects/#{@schema_context_prefix}#{subject}/versions/#{version}",
      expects: [200, 404], body: { schema: schema.to_s }.to_json, query: { verbose: true }, idempotent: true)

    data.fetch('messages', []) unless data.has_key?('error_code')
  end

  # Get global config
  def global_config
    get("/config", idempotent: true)
  end

  # Update global config
  def update_global_config(config)
    put("/config", body: config.to_json, idempotent: true)
  end

  # Get config for subject
  def subject_config(subject)
    get("/config/#{@schema_context_prefix}#{subject}", idempotent: true)
  end

  # Update config for subject
  def update_subject_config(subject, config)
    put("/config/#{@schema_context_prefix}#{subject}", body: config.to_json, idempotent: true)
  end

  private

  def get(path, **options)
    request(path, method: :get, **options)
  end

  def put(path, **options)
    request(path, method: :put, **options)
  end

  def post(path, **options)
    request(path, method: :post, **options)
  end

  def request(path, **options)
    options = { expects: 200 }.merge!(options)
    path = File.join(@path_prefix, path) unless @path_prefix.nil?
    response = @connection.request(path: path, **options)
    JSON.parse(response.body)
  end
end
