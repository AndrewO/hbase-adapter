module HbaseAdapter
  class Table
    attr_reader :connection, :table_name
    
    def initialize(connection, table_name)
      @connection = connection
      @table_name = table_name
    end
    
    def enable!
      connection.client.enableTable(table_name)
    end

    def disable!
      connection.client.disableTable(table_name)
    end

    def enabled?
      connection.client.isTableEnabled(table_name)
    end

    def compact!
      connection.client.compact(table_name)
    end

    def major_compact!
      connection.client.majorCompact(table_name)
    end

    def columns
      connection.client.getColumnDescriptors(table_name).inject({}) do|hash, (col_name, col)|
        hash[col_name.to_sym] = HbaseAdapter::ColumnFamily.new(col)
        hash
      end
    end
    
    def regions
      connection.client.getTableRegions(table_name)
    end
  end
end