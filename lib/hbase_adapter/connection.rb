require 'hbase_adapter/table'

module HbaseAdapter
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
      table_names.inject({}) do|hash, table_name|
        hash[table_name.to_sym] = HbaseAdapter::Table.new(client, table_name)
        hash
      end
    end
    
    def create_table!(table_name, *column_families)
      client.createTable(table_name.to_s, column_families.map {|cd| cd.column_descriptor})
    end
    
    def delete_table!(table_name)
      client.disableTable(table_name.to_s)
      client.deleteTable(table_name.to_s)
    end
  end
end