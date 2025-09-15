# frozen_string_literal: true

require 'avro_turf/test/fake_prefixed_confluent_schema_registry_server'

class AuthorizedFakePrefixedConfluentSchemaRegistryServer < FakePrefixedConfluentSchemaRegistryServer
  set :host_authorization, permitted_hosts: ['example.org', 'registry.example.com']
end
