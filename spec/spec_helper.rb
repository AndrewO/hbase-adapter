require 'rubygems'
require 'spec'

require File.dirname(__FILE__) + '/../lib/hbaser'

HBASE_CONNECTION_PARAMS = ['127.0.0.1', 9090]

# def test_client
#   return @test_client if @test_client
#   t = Thrift::BufferedTransport.new(Thrift::Socket.new(*HBASE_CONNECTION_PARAMS))
#   p = Thrift::BinaryProtocol.new(t)
#   c = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(p)
#   t.open
# 
#   @test_client = c
# end