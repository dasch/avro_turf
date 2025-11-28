# frozen_string_literal: true

require "json"
require "rack"

# Ensure AvroTurf class exists so we can add modules to it
# This is defined as a class (not module) in lib/avro_turf/version.rb
class AvroTurf
  module Test
  end
end

# A lightweight Rack-based router module that provides Sinatra-like DSL.
# This module is designed to replace Sinatra::Base for the fake schema registry servers
# used in testing, eliminating the sinatra dependency.
#
# Usage:
#   class MyServer
#     include AvroTurf::Test::FakeServer
#
#     get "/path/:param" do
#       { result: params[:param] }.to_json
#     end
#
#     post "/other" do
#       halt(404, '{"error": "not found"}') if some_condition
#       '{"ok": true}'
#     end
#   end
#
module AvroTurf::Test::FakeServer
  def self.included(base)
    base.extend(ClassMethods)
    base.include(InstanceMethods)
  end

  module ClassMethods
    # Storage for routes defined in this class
    def routes
      @routes ||= {"GET" => [], "POST" => [], "PUT" => [], "DELETE" => []}
    end

    # When a class inherits from another that includes FakeServer,
    # ensure it gets its own routes hash
    def inherited(subclass)
      super
      subclass.instance_variable_set(:@routes, nil)
    end

    # Define a GET route
    def get(pattern, &block)
      add_route("GET", pattern, block)
    end

    # Define a POST route
    def post(pattern, &block)
      add_route("POST", pattern, block)
    end

    # Define a PUT route
    def put(pattern, &block)
      add_route("PUT", pattern, block)
    end

    # Define a DELETE route
    def delete(pattern, &block)
      add_route("DELETE", pattern, block)
    end

    # Sinatra-compatible `set` method for configuration
    def set(key, value)
      case key
      when :host_authorization
        @host_authorization = value
      else
        instance_variable_set(:"@#{key}", value)
      end
    end

    # Access host authorization settings
    def host_authorization
      @host_authorization
    end

    # Rack interface - creates a new instance and calls it
    def call(env)
      new.call(env)
    end

    private

    def add_route(method, pattern, block)
      routes[method] << [compile_pattern(pattern), pattern, block]
    end

    # Convert a route pattern like "/subjects/:subject/versions" to a regex
    # with named capture groups: /^\/subjects\/(?<subject>[^\/]+)\/versions$/
    def compile_pattern(pattern)
      regex_str = Regexp.escape(pattern).gsub(/:(\w+)/) { "(?<#{$1}>[^/]+)" }
      Regexp.new("^#{regex_str}$")
    end
  end

  module InstanceMethods
    attr_reader :request, :params

    def call(env)
      @request = Rack::Request.new(env)
      @params = {}

      # Check host authorization if configured
      if (auth = self.class.host_authorization)
        permitted = auth[:permitted_hosts] || []
        unless permitted.include?(@request.host)
          return [403, {"Content-Type" => "text/plain"}, ["Forbidden"]]
        end
      end

      # Use catch/throw for halt mechanism (like Sinatra)
      catch(:halt) do
        route_and_dispatch(env)
      end
    end

    # Early return from a route handler with a specific status and body
    def halt(status, body)
      throw :halt, [status, {"Content-Type" => "application/json"}, [body]]
    end

    private

    def route_and_dispatch(env)
      method = env["REQUEST_METHOD"]
      path = env["PATH_INFO"]

      # Parse query string into params (with both string and symbol keys for compatibility)
      query_params = Rack::Utils.parse_query(env["QUERY_STRING"] || "")
      @params = {}
      query_params.each do |key, value|
        @params[key] = value
        @params[key.to_sym] = value
      end

      # Find matching route (check own class first, then ancestors)
      matched = find_route(method, path)

      if matched
        regex, _pattern, block = matched

        # Extract path parameters from the match
        if (match = regex.match(path))
          match.names.each do |name|
            # Store with both symbol and string keys for compatibility
            @params[name.to_sym] = match[name]
            @params[name] = match[name]
          end
        end

        # Execute the route block in the context of this instance
        body = instance_exec(&block)
        [200, {"Content-Type" => "text/html;charset=utf-8"}, [body]]
      else
        [404, {"Content-Type" => "text/plain"}, ["Not Found"]]
      end
    end

    # Find a matching route by searching this class's routes first,
    # then parent classes (to support inheritance)
    def find_route(method, path)
      klass = self.class
      while klass
        if klass.respond_to?(:routes, true) && klass.routes[method]
          klass.routes[method].each do |route|
            regex, _, _ = route
            return route if regex.match(path)
          end
        end
        # Move up the inheritance chain
        klass = klass.superclass
        # Stop if we've gone past classes that include FakeServer
        break unless klass.respond_to?(:routes, true)
      end
      nil
    end
  end
end
