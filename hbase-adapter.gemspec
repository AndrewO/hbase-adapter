# -*- encoding: utf-8 -*-
# 

# The library's version will always match the version of HBase that the thrift file came from.
module HbaseAdapter
  VERSION = "0.0.1"
end

Gem::Specification.new do |s|
  s.name        = "hbase-adapter"
  s.version     = HbaseAdapter::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Andrew O'Brien"]
  s.email       = ["obrien.andrew@gmail.com"]
  s.homepage    = "http://github.com/AndrewO/hbase-adapter"
  s.summary     = "A Ruby wrapper for the HBase Thrift interface."
  s.description = "Wrapping up all of that HBase Thrift stuff so you can use it."

  s.required_rubygems_version = ">= 1.3.6"
  s.rubyforge_project         = "hbase-adapter"

  s.add_dependency "hbase-thrift", "~> 0.20.5"

  s.add_development_dependency "rspec"
  s.add_development_dependency "ruby-debug"
  
  s.files        = Dir.glob("lib/**/*") + %w(LICENSE.txt README.textile)
  s.require_paths = ['lib']
end