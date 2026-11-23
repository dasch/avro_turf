# frozen_string_literal: true

source "https://rubygems.org"

# Specify your gem's dependencies in avro_turf.gemspec
gemspec

gem "gem-release"
gem "standard"

# multi_json < 1.20 (the last versions supporting Ruby < 3.2) passes options to
# JSON.parse as a positional hash, which json >= 3.0 no longer accepts.
gem "json", "< 3" if RUBY_VERSION < "3.2"
