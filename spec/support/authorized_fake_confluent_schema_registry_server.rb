# frozen_string_literal: true

require "avro_turf/test/fake_confluent_schema_registry_server"

class AuthorizedFakeConfluentSchemaRegistryServer < FakeConfluentSchemaRegistryServer
  set :host_authorization, permitted_hosts: ["example.org", "registry.example.com"]
end
