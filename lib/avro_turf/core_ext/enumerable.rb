# frozen_string_literal: true

module Enumerable
  def as_avro
    map(&:as_avro)
  end
end
