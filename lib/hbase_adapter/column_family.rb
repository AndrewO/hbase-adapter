module HbaseAdapter
  class ColumnFamily
    KEY_MAP = {
      :name => :name,
      :max_versions => :maxVersions
    }
    
    class << self
      def translate_hash(hash)
        Hash[*(hash.map {|(k,v)| [KEY_MAP[k.to_sym], v]}.flatten)]
      end
    end
    
    attr_reader :column_descriptor
    def initialize(args = {})
      @column_descriptor = case args
      when Apache::Hadoop::Hbase::Thrift::ColumnDescriptor
        args
      when Hash
        Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new(self.class.translate_hash(args))
      end
    end
    
    def name
      column_descriptor.name
    end
    
    def max_versions
      column_descriptor.maxVersions
    end
    
    # TODO: support other keys
    # TODO: use delegator
  end
end