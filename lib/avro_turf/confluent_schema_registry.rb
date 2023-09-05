require 'excon'

class AvroTurf::ConfluentSchemaRegistry
  CONTENT_TYPE = "application/vnd.schemaregistry.v1+json".freeze

  def initialize(
    url,
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
    path_prefix: nil
  )
    @path_prefix = path_prefix
    @logger = logger
    headers = Excon.defaults[:headers].merge(
      "Content-Type" => CONTENT_TYPE
    )
    headers[:proxy] = proxy unless proxy.nil?
    @connection = Excon.new(
      url,
      headers: headers,
      user: user,
      password: password,
      ssl_ca_file: ssl_ca_file,
      client_cert: client_cert,
      client_key: client_key,
      client_key_pass: client_key_pass,
      client_cert_data: client_cert_data,
      client_key_data: client_key_data
    )
  end

  def fetch(id, **options)
    @logger.info "Fetching schema with id #{id}"
    data = get("/schemas/ids/#{id}", **options)
    data.fetch("schema")
  end

  def register(subject, schema, **options)
    data = post("/subjects/#{subject}/versions", body: { schema: schema.to_s }.to_json, **options)

    id = data.fetch("id")

    @logger.info "Registered schema for subject `#{subject}`; id = #{id}"

    id
  end

  # List all subjects
  def subjects(**options)
    get('/subjects', **options)
  end

  # List all versions for a subject
  def subject_versions(subject, **options)
    get("/subjects/#{subject}/versions", **options)
  end

  # Get a specific version for a subject
  def subject_version(subject, version = 'latest', **options)
    get("/subjects/#{subject}/versions/#{version}", **options)
  end

  # Get the subject and version for a schema id
  def schema_subject_versions(schema_id, **options)
    get("/schemas/ids/#{schema_id}/versions", **options)
  end

  # Check if a schema exists. Returns nil if not found.
  def check(subject, schema, **options)
    data = post("/subjects/#{subject}",
                expects: [200, 404],
                body: { schema: schema.to_s }.to_json,
                **options)
    data unless data.has_key?("error_code")
  end

  # Check if a schema is compatible with the stored version.
  # Returns:
  # - true if compatible
  # - nil if the subject or version does not exist
  # - false if incompatible
  # http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#compatibility
  def compatible?(subject, schema, version = 'latest', **options)
    data = post("/compatibility/subjects/#{subject}/versions/#{version}",
                expects: [200, 404], body: { schema: schema.to_s }.to_json, **options)
    data.fetch('is_compatible', false) unless data.has_key?('error_code')
  end

  # Get global config
  def global_config(**options)
    get("/config", **options)
  end

  # Update global config
  def update_global_config(config, **options)
    put("/config", body: config.to_json, **options)
  end

  # Get config for subject
  def subject_config(subject, **options)
    get("/config/#{subject}", **options)
  end

  # Update config for subject
  def update_subject_config(subject, config, **options)
    put("/config/#{subject}", body: config.to_json, **options)
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
