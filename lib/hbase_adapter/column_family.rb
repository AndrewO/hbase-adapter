module HbaseAdapter
  class ColumnFamily
    attr_reader :column_descriptor
    def initialize(column_descriptor)
      @column_descriptor = case column_descriptor
      when Apache::Hadoop::Hbase::Thrift::ColumnDescriptor
        column_descriptor
      when Hash
        Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new(column_descriptor)
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