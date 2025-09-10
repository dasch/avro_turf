# frozen_string_literal: true

class Time
  def as_avro
    iso8601
  end
end
