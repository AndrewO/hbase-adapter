module Hbaser
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
      connection.client.getColumnDescriptors(table_name)
    end
    
    def regions
      connection.client.getTableRegions(table_name)
    end

    def delete!
      connection.client.deleteTable(table_name)
    end
  end
end