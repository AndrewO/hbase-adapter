$:.unshift File.expand_path(File.dirname(__FILE__))

require 'rubygems'
require 'hbase-thrift'

module HbaseAdapter
  IOError = Apache::Hadoop::Hbase::Thrift::IOError
  IllegalArgument = Apache::Hadoop::Hbase::Thrift::IllegalArgument
  TableAlreadyExists = Apache::Hadoop::Hbase::Thrift::AlreadyExists
end

require 'hbase_adapter/connection'
require 'hbase_adapter/table'
require 'hbase_adapter/column_family'
require 'hbase_adapter/row'