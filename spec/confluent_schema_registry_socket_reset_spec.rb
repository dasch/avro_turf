# frozen_string_literal: true

require "avro_turf/confluent_schema_registry"

describe AvroTurf::ConfluentSchemaRegistry, "socket reset on interrupt" do
  let(:registry) { described_class.new("http://127.0.0.1:#{port}", logger: logger) }
  let(:server) { TCPServer.new("127.0.0.1", 0) }
  let(:port) { server.addr[1] }
  let(:logger) { Logger.new(StringIO.new) }

  let(:timeout_exception) { Class.new(Exception) } # rubocop:disable Lint/InheritException

  # These specs need an actual socket to the local test server
  around do |example| 
    FakeFS.deactivate!
    WebMock.allow_net_connect! if defined?(WebMock)

    example.call
  ensure
    WebMock.disable_net_connect! if defined?(WebMock)
    FakeFS.activate!
  end

  before do
    @server_thread = Thread.new do
      loop do
        socket = server.accept

        Thread.new(socket) do |connection|
          request_line = connection.gets
          path = request_line.split[1]

          schema = {type: "string", path: path }.to_json
          body = { schema: schema }.to_json

          connection.write(
            "HTTP/1.1 200 OK\r\n" \
            "Content-Type: application/json\r\n" \
            "Content-Length: #{body.bytesize}\r\n\r\n" \
            "#{body}"
          )
          connection.flush
        end
      end
    end

    timeout = true
    this = self
    Excon::Socket.class_eval do
      alias_method :__unstubbed_readline, :readline
      define_method(:readline) do |*args|
        if timeout
          timeout = false
          raise this.timeout_exception, "request timeout"
        end

        __unstubbed_readline(*args)
      end
    end
  end

  after do
    Excon::Socket.class_eval do
      alias_method :readline, :__unstubbed_readline
      remove_method :__unstubbed_readline
    end

    @server_thread&.kill
    server.close unless server.closed?
  end

  it "does not hand the next request the abandoned response" do
    expect { registry.fetch("1") }.to raise_error(timeout_exception)
    expect(registry.fetch("2")).to include("/schemas/ids/2")
  end
end
