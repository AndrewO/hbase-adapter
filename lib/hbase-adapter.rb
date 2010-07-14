$:.unshift File.expand_path(File.dirname(__FILE__))

require 'rubygems'
require 'hbase-thrift'

module HbaseAdapter
end

require 'hbase_adapter/connection'
require 'hbase_adapter/table'