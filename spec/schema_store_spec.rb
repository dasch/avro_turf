require 'avro_turf/schema_store'

describe AvroTurf::SchemaStore do
  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  describe "#find" do
    subject(:store) { described_class.new(path: "spec/schemas") }

    context '.avsc filetype' do
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

      it 'only finds schemas with the predefined filetype' do
        define_schema "person.json", <<-JSON
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
        JSON

        expect {
          store.find("person")
        }.to raise_exception(AvroTurf::SchemaNotFoundError)
      end
    end

    context '.json filetype' do
      subject(:store) { described_class.new(path: "spec/schemas", filetype: :json) }

      it "finds schemas on the file system" do
        define_schema "message.json", <<-JSON
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
        JSON

        schema = store.find("message")
        expect(schema.fullname).to eq "message"
      end

      it "resolves missing references" do
        define_schema "person.json", <<-JSON
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
        JSON

        define_schema "address.json", <<-JSON
          {
            "type": "record",
            "name": "address",
            "fields": []
          }
        JSON

        schema = store.find("person")

        expect(schema.fullname).to eq "person"
      end

      it "finds namespaced schemas" do
        FileUtils.mkdir_p("spec/schemas/test/people")

        define_schema "test/people/person.json", <<-JSON
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
        JSON

        define_schema "test/people/address.json", <<-JSON
          {
            "name": "address",
            "namespace": "test.people",
            "type": "record",
            "fields": []
          }
        JSON

        schema = store.find("person", "test.people")

        expect(schema.fullname).to eq "test.people.person"
      end

      it "ignores the namespace when the name contains a dot" do
        FileUtils.mkdir_p("spec/schemas/test/acme")

        define_schema "test/acme/message.json", <<-JSON
          {
            "name": "message",
            "namespace": "test.acme",
            "type": "record",
            "fields": []
          }
        JSON

        schema = store.find("test.acme.message", "test.yolo")

        expect(schema.fullname).to eq "test.acme.message"
      end

      it "raises AvroTurf::SchemaNotFoundError if there's no schema file matching the name" do
        expect {
          store.find("not_there")
        }.to raise_error(AvroTurf::SchemaNotFoundError, "could not find Avro schema at `spec/schemas/not_there.json'")
      end

      it "raises AvroTurf::SchemaNotFoundError if a type reference cannot be resolved" do
        define_schema "person.json", <<-JSON
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
        JSON

        expect {
          store.find("person")
        }.to raise_exception(AvroTurf::SchemaNotFoundError)
      end

      it "raises AvroTurf::SchemaError if the schema's namespace doesn't match the file location" do
        FileUtils.mkdir_p("spec/schemas/test/people")

        define_schema "test/people/person.json", <<-JSON
          {
            "name": "person",
            "namespace": "yoyoyo.nanana",
            "type": "record",
            "fields": []
          }
        JSON

        expect {
          store.find("test.people.person")
        }.to raise_error(AvroTurf::SchemaError, "expected schema `spec/schemas/test/people/person.json' to define type `test.people.person'")
      end

      it "handles circular dependencies" do
        define_schema "a.json", <<-JSON
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
        JSON

        define_schema "b.json", <<-JSON
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
        JSON

        schema = store.find("a")
        expect(schema.fullname).to eq "a"
      end

      it "caches schemas in memory" do
        define_schema "person.json", <<-JSON
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
        JSON

        # Warm the schema cache.
        store.find("person")

        # Force a failure if the schema file is read again.
        FileUtils.rm("spec/schemas/person.json")

        schema = store.find("person")
        expect(schema.fullname).to eq "person"
      end

      it 'only finds schemas with the predefined filetype' do
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

        expect {
          store.find("person")
        }.to raise_exception(AvroTurf::SchemaNotFoundError)
      end
    end
  end

  describe "#load_schemas!" do
    context '.avsc filetype' do
      subject(:store) { described_class.new(path: "spec/schemas") }

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

    context '.json filetype' do
      subject(:store) { described_class.new(path: "spec/schemas", filetype: :json) }

      it "loads schemas defined in the `schemas_path` directory" do
        define_schema "person.json", <<-JSON
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
        JSON

        # Warm the schema cache.
        store.load_schemas!

        # Force a failure if the schema file is read again.
        FileUtils.rm("spec/schemas/person.json")

        schema = store.find("person")
        expect(schema.fullname).to eq "person"
      end

      it "recursively finds schema definitions in subdirectories" do
        FileUtils.mkdir_p("spec/schemas/foo/bar")

        define_schema "foo/bar/person.json", <<-JSON
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
        JSON

        # Warm the schema cache.
        store.load_schemas!

        # Force a failure if the schema file is read again.
        FileUtils.rm("spec/schemas/foo/bar/person.json")

        schema = store.find("foo.bar.person")
        expect(schema.fullname).to eq "foo.bar.person"
      end
    end
  end
end
