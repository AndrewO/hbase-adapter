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
      
      c
    end

    def tables
    end
  end
end