$:.unshift File.expand_path(File.dirname(__FILE__))

require 'rubygems'
require 'hbase-thrift'

module Hbaser
end

require 'hbaser/connection'
require 'hbaser/table'