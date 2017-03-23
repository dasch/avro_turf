require 'avro_turf/schema_store'

describe AvroTurf::RegistrySchemaStore do
  subject(:store) { described_class.new("http://reg_store") }

  describe "#find" do
    subject(:found) { store.find("hello") }
    context "when shema exists" do
      before do
        stub_request(
          :get, "http://reg_store/subjects/hello/versions/latest"
        ).to_return(
          status: 200,
          body: '{"subject":"hello","version":1,"id":23,"schema":"{\"type\":\"record\",\"name\":\"hello\",\"fields\":[{\"name\":\"hello\",\"type\":\"string\"}]}"}'
        )
      end

      it { is_expected.to be_a Avro::Schema }
    end

    context "when schema is missing" do
      subject { -> { found } }
       before do
        stub_request(
          :get, "http://reg_store/subjects/hello/versions/latest"
        ).to_return(status: 404)
      end

      it { is_expected.to raise_exception AvroTurf::SchemaNotFoundError }
    end
  end

  describe "#load_schemas!" do
    #  Testing the side effect
    subject { store.instance_variable_get(:@schemas) }

    before do
       stub_request(:get, "http://reg_store/subjects").
         to_return(
          status: 200,
          body: "[\"foo\", \"bar\"]"
        )

       stub_request(:get, "http://reg_store/subjects/foo/versions/latest").
         to_return(
          status: 200,
          body: '{"subject":"foo","version":1,"id":23,"schema":"{\"type\":\"record\",\"name\":\"foo\",\"fields\":[{\"name\":\"foo\",\"type\":\"string\"}]}"}'
        )

       stub_request(:get, "http://reg_store/subjects/bar/versions/latest").
         to_return(
          status: 200,
          body: '{"subject":"bar","version":1,"id":23,"schema":"{\"type\":\"record\",\"name\":\"bar\",\"fields\":[{\"name\":\"bar\",\"type\":\"string\"}]}"}'
        )

        store.load_schemas!
    end

    it { is_expected.to include "foo", "bar" }
  end
end
