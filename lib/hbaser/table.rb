module Hbaser
  class Table
    attr_reader :client, :table_name
    
    def initialize(client, table_name)
      @client = client
      @table_name = table_name
    end
    
    def enable!
      client.enableTable(table_name)
    end

    def disable!
      client.disableTable(table_name)
    end

    def enabled?
      client.isTableEnabled(table_name)
    end

    def compact!
      client.compact(table_name)
    end

    def major_compact!
      client.majorCompact(table_name)
    end

    def columns
      client.getColumnDescriptions(table_name)
    end
    
    def regions
      client.getTableRegions(table_name)
    end

    def delete!
      client.deleteTable(table_name)
    end
  end
end