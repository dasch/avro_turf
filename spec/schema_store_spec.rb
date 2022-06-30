require 'avro_turf/schema_store'

describe AvroTurf::SchemaStore do
  let(:store) { AvroTurf::SchemaStore.new(path: "spec/schemas") }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  describe "#find" do
    it "finds schemas on the file system" do
      define_schema "message.avsc", <<-AVSC
        {
          "name": "message",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "message"
            }
          ]
        }
      AVSC

      schema = store.find("message")
      expect(schema.fullname).to eq "message"
    end

    it "resolves missing references when nested schema is not a named type" do
      define_schema "root.avsc", <<-AVSC
        {
          "type": "record",
          "name": "root",
          "fields": [
            {
              "type": "nested",
              "name": "nested_value"
            }
          ]
        }
      AVSC

      define_schema "nested.avsc", <<-AVSC
        {
          "name": "nested",
          "type": "string",
          "logicalType": "uuid"
        }
      AVSC

      schema = store.find("root")

      expect(schema.fullname).to eq "root"
    end

    it "resolves missing references" do
      define_schema "person.avsc", <<-AVSC
        {
          "name": "person",
          "type": "record",
          "fields": [
            {
              "name": "address",
              "type": "address"
            }
          ]
        }
      AVSC

      define_schema "address.avsc", <<-AVSC
        {
          "type": "record",
          "name": "address",
          "fields": []
        }
      AVSC

      schema = store.find("person")

      expect(schema.fullname).to eq "person"
    end

    it "finds namespaced schemas" do
      FileUtils.mkdir_p("spec/schemas/test/people")

      define_schema "test/people/person.avsc", <<-AVSC
        {
          "name": "person",
          "namespace": "test.people",
          "type": "record",
          "fields": [
            {
              "name": "address",
              "type": "test.people.address"
            }
          ]
        }
      AVSC

      define_schema "test/people/address.avsc", <<-AVSC
        {
          "name": "address",
          "namespace": "test.people",
          "type": "record",
          "fields": []
        }
      AVSC

      schema = store.find("person", "test.people")

      expect(schema.fullname).to eq "test.people.person"
    end

    it "ignores the namespace when the name contains a dot" do
      FileUtils.mkdir_p("spec/schemas/test/acme")

      define_schema "test/acme/message.avsc", <<-AVSC
        {
          "name": "message",
          "namespace": "test.acme",
          "type": "record",
          "fields": []
        }
      AVSC

      schema = store.find("test.acme.message", "test.yolo")

      expect(schema.fullname).to eq "test.acme.message"
    end

    it "raises AvroTurf::SchemaNotFoundError if there's no schema file matching the name" do
      expect {
        store.find("not_there")
      }.to raise_error(AvroTurf::SchemaNotFoundError, "could not find Avro schema at `spec/schemas/not_there.avsc'")
    end

    it "raises AvroTurf::SchemaNotFoundError if a type reference cannot be resolved" do
      define_schema "person.avsc", <<-AVSC
        {
          "name": "person",
          "type": "record",
          "fields": [
            {
              "name": "address",
              "type": "address"
            }
          ]
        }
      AVSC

      expect {
        store.find("person")
      }.to raise_exception(AvroTurf::SchemaNotFoundError)
    end

    it "raises AvroTurf::SchemaError if the schema's namespace doesn't match the file location" do
      FileUtils.mkdir_p("spec/schemas/test/people")

      define_schema "test/people/person.avsc", <<-AVSC
        {
          "name": "person",
          "namespace": "yoyoyo.nanana",
          "type": "record",
          "fields": []
        }
      AVSC

      expect {
        store.find("test.people.person")
      }.to raise_error(AvroTurf::SchemaError, "expected schema `spec/schemas/test/people/person.avsc' to define type `test.people.person'")
    end

    it "handles circular dependencies" do
      define_schema "a.avsc", <<-AVSC
        {
          "name": "a",
          "type": "record",
          "fields": [
            {
              "type": "b",
              "name": "b"
            }
          ]
        }
      AVSC

      define_schema "b.avsc", <<-AVSC
        {
          "name": "b",
          "type": "record",
          "fields": [
            {
              "type": "a",
              "name": "a"
            }
          ]
        }
      AVSC

      schema = store.find("a")
      expect(schema.fullname).to eq "a"
    end

    it "caches schemas in memory" do
      define_schema "person.avsc", <<-AVSC
        {
          "name": "person",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "full_name"
            }
          ]
        }
      AVSC

      # Warm the schema cache.
      store.find("person")

      # Force a failure if the schema file is read again.
      FileUtils.rm("spec/schemas/person.avsc")

      schema = store.find("person")
      expect(schema.fullname).to eq "person"
    end

    # This test would fail under avro_turf <= v0.11.0
    it "does NOT cache *nested* schemas in memory" do
      FileUtils.mkdir_p("spec/schemas/test")

      define_schema "test/person.avsc", <<-AVSC
        {
          "name": "person",
          "namespace": "test",
          "type": "record",
          "fields": [
            {
              "name": "address",
              "type": {
                "name": "address",
                "type": "record",
                "fields": [
                  { "name": "addr1", "type": "string" },
                  { "name": "addr2", "type": "string" },
                  { "name": "city", "type": "string" },
                  { "name": "zip", "type": "string" }
                ]
              }
            }
          ]
        }
      AVSC

      schema = store.find('person', 'test')
      expect(schema.fullname).to eq "test.person"

      expect { store.find('address', 'test') }.
        to raise_error(AvroTurf::SchemaNotFoundError)
    end

    # This test would fail under avro_turf <= v0.11.0
    it "allows two different avsc files to define nested sub-schemas with the same fullname" do
      FileUtils.mkdir_p("spec/schemas/test")

      define_schema "test/person.avsc", <<-AVSC
        {
          "name": "person",
          "namespace": "test",
          "type": "record",
          "fields": [
            {
              "name": "location",
              "type": {
                "name": "location",
                "type": "record",
                "fields": [
                  { "name": "city", "type": "string" },
                  { "name": "zipcode", "type": "string" }
                ]
              }
            }
          ]
        }
      AVSC

      define_schema "test/company.avsc", <<-AVSC
        {
          "name": "company",
          "namespace": "test",
          "type": "record",
          "fields": [
            {
              "name": "headquarters",
              "type": {
                "name": "location",
                "type": "record",
                "fields": [
                  { "name": "city", "type": "string" },
                  { "name": "postcode", "type": "string" }
                ]
              }
            }
          ]
        }
      AVSC

      company = nil
      person = store.find('person', 'test')

      # This should *NOT* raise the error:
      # #<Avro::SchemaParseError: The name "test.location" is already in use.>
      expect { company = store.find('company', 'test') }.not_to raise_error

      person_location_field = person.fields_hash['location']
      expect(person_location_field.type.name).to eq('location')
      expect(person_location_field.type.fields_hash).to include('zipcode')
      expect(person_location_field.type.fields_hash).not_to include('postcode')

      company_headquarters_field = company.fields_hash['headquarters']
      expect(company_headquarters_field.type.name).to eq('location')
      expect(company_headquarters_field.type.fields_hash).to include('postcode')
      expect(company_headquarters_field.type.fields_hash).not_to include('zipcode')
    end

    it "is thread safe" do
      define_schema "address.avsc", <<-AVSC
        {
          "type": "record",
          "name": "address",
          "fields": []
        }
      AVSC

      # Set a Thread breakpoint right in the core place of race condition
      expect(Avro::Name)
        .to receive(:add_name)
        .and_wrap_original { |m, *args|
          Thread.stop
          m.call(*args)
        }

      # Run two concurring threads which both will trigger the same schema loading
      threads = 2.times.map { Thread.new { store.find("address") } }
      # Wait for the moment when both threads will reach the breakpoint
      sleep 0.001 until threads.all?(&:stop?)

      expect {
        # Resume the threads evaluation, one after one
        threads.each do |thread|
          next unless thread.status == 'sleep'

          thread.run
          sleep 0.001 until thread.stop?
        end

        # Ensure that threads are finished
        threads.each(&:join)
      }.to_not raise_error
    end
  end

  describe "#load_schemas!" do
    it "loads schemas defined in the `schemas_path` directory" do
      define_schema "person.avsc", <<-AVSC
        {
          "name": "person",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "full_name"
            }
          ]
        }
      AVSC

      # Warm the schema cache.
      store.load_schemas!

      # Force a failure if the schema file is read again.
      FileUtils.rm("spec/schemas/person.avsc")

      schema = store.find("person")
      expect(schema.fullname).to eq "person"
    end

    it "recursively finds schema definitions in subdirectories" do
      FileUtils.mkdir_p("spec/schemas/foo/bar")

      define_schema "foo/bar/person.avsc", <<-AVSC
        {
          "name": "foo.bar.person",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "full_name"
            }
          ]
        }
      AVSC

      # Warm the schema cache.
      store.load_schemas!

      # Force a failure if the schema file is read again.
      FileUtils.rm("spec/schemas/foo/bar/person.avsc")

      schema = store.find("foo.bar.person")
      expect(schema.fullname).to eq "foo.bar.person"
    end
  end
end
