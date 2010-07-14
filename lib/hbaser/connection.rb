require 'hbaser/table'

module Hbaser
  class Connection
    attr_reader :host, :port

    def initialize(options)
      @host = options[:host] || options["host"]
      @port = options[:port] || options["port"]
      
      @client = nil
    end

    def connected? 
      @client
    end

    def client
      connect! unless connected?
      @client
    end

    def connect!
      t = Thrift::BufferedTransport.new(Thrift::Socket.new(host, port))
      p = Thrift::BinaryProtocol.new(t)
      c = Apache::Hadoop::Hbase::Thrift::Hbase::Client.new(p)
      t.open
      
      @client = c
    end

    def table_names
      client.getTableNames
    end
    
    def tables
      table_names.map {|table_name| Hbaser::Table.new(client, table_name)}
    end
  end
end