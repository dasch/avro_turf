# frozen_string_literal: true

require "webmock/rspec"
require "avro_turf/confluent_schema_registry"
require "avro_turf/test/fake_confluent_schema_registry_server"

describe AvroTurf::ConfluentSchemaRegistry do
  let(:user) { "abc" }
  let(:password) { "xxyyzz" }
  let(:client_cert) { "test client cert" }
  let(:client_chain) { "test client cert chain" }
  let(:client_key) { "test client key" }
  let(:client_key_pass) { "test client key password" }
  let(:connect_timeout) { 10 }

  context "authenticated by cert" do
    it_behaves_like "a confluent schema registry client" do
      let(:registry) {
        described_class.new(
          registry_url,
          logger: logger,
          client_cert: client_cert,
          client_key: client_key,
          client_key_pass: client_key_pass
        )
      }
    end
  end

  context "authenticated by cert with chain" do
    it_behaves_like "a confluent schema registry client" do
      let(:registry) {
        described_class.new(
          registry_url,
          logger: logger,
          client_cert: client_cert,
          client_chain: client_chain,
          client_key: client_key,
          client_key_pass: client_key_pass
        )
      }
    end
  end

  context "authenticated by basic auth" do
    it_behaves_like "a confluent schema registry client" do
      let(:registry) {
        described_class.new(
          registry_url,
          logger: logger,
          user: user,
          password: password
        )
      }
    end
  end

  context "with connect_timeout" do
    it_behaves_like "a confluent schema registry client" do
      let(:registry) {
        described_class.new(
          registry_url,
          logger: logger,
          user: user,
          password: password,
          connect_timeout: connect_timeout
        )
      }
    end
  end

  context "with non default schema_context" do
    it_behaves_like "a confluent schema registry client", schema_context: "other" do
      let(:registry) {
        described_class.new(
          registry_url,
          logger: logger,
          schema_context: "other",
          user: user,
          password: password,
          connect_timeout: connect_timeout
        )
      }
    end
  end
end
