# -*- coding: utf-8 -*-
$:.unshift(File.expand_path("lib", File.dirname(__FILE__)))
require 'mosql/version'

Gem::Specification.new do |gem|
  gem.authors          = ["Nelson Elhage", "Mohamed Osama"]
  gem.email            = ["nelhage@stripe.com", "oss@findhotel.net"]
  gem.description      = %q{A library for streaming MongoDB to PostgreSQL}
  gem.summary          = %q{MongoDB -> PostgreSQL streaming bridge}
  # Forked from: https://github.com/stripe/mosql
  gem.homepage         = "https://github.com/FindHotel/mosql"

  gem.required_ruby_version     = ">= 2.0.0"

  gem.license       = "MIT"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "mosql"
  gem.require_paths = ["lib"]
  gem.version       = MoSQL::VERSION

  gem.add_runtime_dependency "sequel", "~> 4.44"
  gem.add_runtime_dependency "pg", "~> 0.20"
  gem.add_runtime_dependency "rake", "~> 12.0"
  gem.add_runtime_dependency "log4r", "~> 1.1"
  gem.add_runtime_dependency "json", "~> 2.0"

  gem.add_runtime_dependency "mongoriver", "0.4"

  gem.add_runtime_dependency "mongo", "~> 1.12"
  gem.add_runtime_dependency "bson", "~> 1.12"
  gem.add_runtime_dependency "bson_ext", "~> 1.12"

  gem.add_development_dependency "minitest", "~> 5.10"
  gem.add_development_dependency "mocha", "~> 1.2"
  gem.add_development_dependency "rake-notes", "~> 0.2"
  gem.add_development_dependency "byebug", "~> 9.0"
end
