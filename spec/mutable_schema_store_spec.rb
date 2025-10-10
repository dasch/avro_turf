# frozen_string_literal: true

require "avro_turf/mutable_schema_store"

describe AvroTurf::MutableSchemaStore do
  let(:store) { AvroTurf::MutableSchemaStore.new(path: "spec/schemas") }

  describe "#add_schema" do
    it "adds a schema to the store" do
      schema_hash = {
        "name" => "person",
        "namespace" => "test.people",
        "type" => "record",
        "fields" => [
          {
            "type" => "string",
            "name" => "name"
          }
        ]
      }

      schema = store.add_schema(schema_hash)
      expect(schema.fullname).to eq "test.people.person"
      expect(store.schemas["test.people.person"]).to eq schema
    end

    # This test would fail under avro_turf <= v0.19.0
    it "does NOT cache *nested* schemas in memory" do
      schema_hash = {
        "name" => "person",
        "namespace" => "test",
        "type" => "record",
        "fields" => [
          {
            "name" => "address",
            "type" => {
              "name" => "address",
              "type" => "record",
              "fields" => [
                {"name" => "addr1", "type" => "string"},
                {"name" => "addr2", "type" => "string"},
                {"name" => "city", "type" => "string"},
                {"name" => "zip", "type" => "string"}
              ]
            }
          }
        ]
      }

      schema = store.add_schema(schema_hash)
      expect(schema.fullname).to eq "test.person"
      expect(store.schemas["test.person"]).to eq schema

      expect { store.find("address", "test") }
        .to raise_error(AvroTurf::SchemaNotFoundError)
    end

    # This test would fail under avro_turf <= v1.19.0
    it "allows two different schemas to define nested sub-schemas with the same fullname" do
      person_schema = {
        "name" => "person",
        "namespace" => "test",
        "type" => "record",
        "fields" => [
          {
            "name" => "location",
            "type" => {
              "name" => "location",
              "type" => "record",
              "fields" => [
                {"name" => "city", "type" => "string"},
                {"name" => "zipcode", "type" => "string"}
              ]
            }
          }
        ]
      }

      company_schema = {
        "name" => "company",
        "namespace" => "test",
        "type" => "record",
        "fields" => [
          {
            "name" => "headquarters",
            "type" => {
              "name" => "location",
              "type" => "record",
              "fields" => [
                {"name" => "city", "type" => "string"},
                {"name" => "postcode", "type" => "string"}
              ]
            }
          }
        ]
      }

      person = store.add_schema(person_schema)

      # This should *NOT* raise the error:
      # #<Avro::SchemaParseError: The name "test.location" is already in use.>
      expect { store.add_schema(company_schema) }.not_to raise_error

      company = store.schemas["test.company"]

      person_location_field = person.fields_hash["location"]
      expect(person_location_field.type.name).to eq("location")
      expect(person_location_field.type.fields_hash).to include("zipcode")
      expect(person_location_field.type.fields_hash).not_to include("postcode")

      company_headquarters_field = company.fields_hash["headquarters"]
      expect(company_headquarters_field.type.name).to eq("location")
      expect(company_headquarters_field.type.fields_hash).to include("postcode")
      expect(company_headquarters_field.type.fields_hash).not_to include("zipcode")
    end
  end

  describe "#schemas" do
    it "provides access to the internal schemas hash" do
      expect(store.schemas).to be_a(Hash)
      expect(store.schemas).to be_empty

      schema_hash = {
        "name" => "test",
        "type" => "record",
        "fields" => []
      }

      store.add_schema(schema_hash)
      expect(store.schemas.size).to eq 1
      expect(store.schemas["test"]).to be_a(Avro::Schema::RecordSchema)
    end
  end
end
