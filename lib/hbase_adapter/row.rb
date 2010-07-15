require 'hbase_adapter/cell'
require 'hbase_adapter/mutation'

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
      timestamp = options[:timestamp]

      if num_versions.nil? && timestamp.nil?
        # One column, no options.  Simple get.  Return a single cell
        tcell = connection.client.get(table.name, key, column).first
        HbaseAdapter::Cell.new(connection, self, column, tcell) if tcell
      else
        if timestamp.nil?
          # One column, versions specified.  getVer
          tcells = connection.client.getVer(table.name, key, column, num_versions)
        else
          # One column, versions specified, max time specified.  getVerTs
          tcells = connection.client.getVerTs(table.name, key, column, timestamp.to_i64, num_versions)
        end
        
        tcells.compact.map {|tcell| HbaseAdapter::Cell.new(connection, self, column, tcell)}
      end
    end
    
    def cells(*columns)
      options = columns.pop if columns.last.is_a?(Hash)
      options ||= {}
      timestamp = options[:timestamp]
      
      if columns.empty?
        if timestamp.nil?
          # getRow
          trow_results = connection.client.getRow(table.name, key)
        else
          # getRowTs
          trow_results = connection.client.getRowTs(table.name, key, timestamp.to_i64)
        end
      else
        if timestamp.nil?
          # getRowWithColumns
          trow_results = connection.client.getRowWithColumns(table.name, key, columns)
        else
          # getRowWithColumnsTs
          trow_results = connection.client.getRowWithColumnsTs(table.name, key, columns, timestamp.to_i64)
        end
      end

      unless trow_results.empty?
        trow_results.first.columns.inject({}) do |hash, (col_name, tcell)|
          hash[col_name.to_sym] = HbaseAdapter::Cell.new(connection, self, col_name, tcell) if tcell
          hash
        end
      end
    end
    
    def mutate!(key, options = {}, &blk)
      timestamp = options[:timestamp]
      
      ml = HbaseAdapter::MutationList.new(&blk)
      
      if timestamp.nil?
        connection.client.mutateRows(table_name, ml.to_thrift)
      else
        connection.client.mutateRowsTs(table_name, ml.to_thrift, timestamp.to_i64)
      end
    end
  end
end