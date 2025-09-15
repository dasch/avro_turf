# frozen_string_literal: true

describe Date, "#as_avro" do
  it "returns Date object describing the time" do
    date = Date.today
    expect(date.as_avro).to eq(date)
  end
end
