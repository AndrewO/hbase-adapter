require 'hbase_adapter/cell'

module HbaseAdapter
  class Row
    attr_reader :connection, :table, :key

    def initialize(connection, table, key)
      @connection = connection
      @table = table
      @key = key
    end

    def [](column, options = {})
      num_versions = options[:num_versions]
      max_time = options[:max_time]

      if num_versions.nil? && max_time.nil?
        # One column, no options.  Simple get.  Return a single cell
        tcell = connection.client.get(table.name, key, column).first
        HbaseAdapter::Cell.new(connection, self, column, tcell)
      else
        if max_time.nil?
          # One column, versions specified.  getVer
          tcells = connection.client.getVer(table.name, key, column, numVersions)
        else
          # One column, versions specified, max time specified.  getVerTs
          tcells = connection.client.getVerTs(table.name, key, column, numVersions, max_time)
        end
        
        tcells.map {|tcell| HbaseAdapter::Cell.new(connection, self, column, tcell)}
      end
    end
    
    def cells(columns = [], options = {})
      if columns.is_a?(Hash) && options.empty?
        options = columns
        columns = []
      end
      
      max_time = options[:max_time]
      
      if columns.empty?
        if max_time.nil?
          # getRow
          trow_results = connection.client.getRow(table.name, key)
        else
          # getRowTs
          trow_results = connection.client.getRowTs(table.name, key, max_time)
        end
      else
        if max_time.nil?
          # getRowWithColumns
          trow_results = connection.client.getRowWithColumns(table.name, key, columns)
        else
          # getRowWithColumnsTs
          trow_results = connection.client.getRowWithColumnsTs(table.name, key, columns, max_time)
        end
      end

      trow_results.first.columns.inject({}) do |hash, (col_name, tcell)|
        hash[col_name.to_sym] = HbaseAdapter::Cell.new(connection, self, col_name, tcell)
        hash
      end
    end
  end
end