describe AvroTurf::Model, ".build" do
  let(:avro) { AvroTurf.new(schemas_path: "spec/schemas/") }

  before do
    FileUtils.mkdir_p("spec/schemas")
  end

  it "adds accessors for each field in a record schema" do
    define_schema "person.avsc", <<-AVSC
      {
        "name": "person",
        "type": "record",
        "fields": [
          {
            "name": "name",
            "type": "string"
          }
        ]
      }
    AVSC

    klass = AvroTurf::Model.build(avro: avro, schema_name: "person")
    person = klass.new(name: "Jane")

    expect(person.name).to eq "Jane"

    person.name = "Karen"

    expect(person.name).to eq "Karen"
  end

  it "adds constants for enums" do
    define_schema "card.avsc", <<-AVSC
      {
        "name": "card",
        "type": "record",
        "fields": [
          {
            "name": "suit",
            "type": {
              "type": "enum",
              "name": "suit",
              "symbols": ["hearts", "spades", "clubs", "diamonds"]
            }
          }
        ]
      }
    AVSC

    klass = AvroTurf::Model.build(avro: avro, schema_name: "card")

    expect(klass::DIAMONDS).to eq "diamonds"

    card = klass.new(suit: "diamonds")

    expect(card.suit).to eq "diamonds"
  end

  it "allows nested schemas" do
    define_schema "person.avsc", <<-AVSC
      {
        "name": "person",
        "type": "record",
        "fields": [
          {
            "name": "address",
            "type": {
              "type": "record",
              "name": "address",
              "fields": [
                {
                  "name": "street",
                  "type": "string"
                },
                {
                  "name": "city",
                  "type": "string"
                }
              ]
            }
          }
        ]
      }
    AVSC

    klass = AvroTurf::Model.build(avro: avro, schema_name: "person")

    expect(klass::Address).not_to be_nil

    person = klass.new(address: { street: "Snaregade 12", city: "Copenhagen" })

    expect(person.address.street).to eq "Snaregade 12"
    expect(person.address.city).to eq "Copenhagen"
  end

  it "handles recursive schemas" do
    define_schema "cons.avsc", <<-AVSC
      {
        "name": "cons",
        "type": "record",
        "fields": [
          {
            "name": "value",
            "type": "int"
          },
          {
            "name": "next",
            "type": ["cons", "null"]
          }
        ]
      }
    AVSC

    klass = AvroTurf::Model.build(avro: avro, schema_name: "cons")

    list = klass.new(value: 1, next: klass.new(value: 2))

    expect(list.value).to eq 1
    expect(list.next.value).to eq 2
  end

  it "allows encoding a model instance" do
    define_schema "person.avsc", <<-AVSC
      {
        "name": "person",
        "type": "record",
        "fields": [
          {
            "name": "address",
            "type": {
              "type": "record",
              "name": "address",
              "fields": [
                {
                  "name": "street",
                  "type": "string"
                },
                {
                  "name": "city",
                  "type": "string"
                }
              ]
            }
          }
        ]
      }
    AVSC

    klass = AvroTurf::Model.build(avro: avro, schema_name: "person")
    person = klass.new(address: { street: "Snaregade 12", city: "Copenhagen" })

    expect(klass.decode(person.encode)).to eq person
  end

  it "allows adding custom methods to the class" do
    define_schema "person.avsc", <<-AVSC
      {
        "name": "person",
        "type": "record",
        "fields": [
          {
            "name": "name",
            "type": "string"
          }
        ]
      }
    AVSC

    klass = AvroTurf::Model.build(avro: avro, schema_name: "person") do
      def first_name
        name.split(" ").first
      end
    end

    person = klass.new(name: "Jane Doe")

    expect(person.first_name).to eq "Jane"
  end
end
