module HbaseAdapter
  class Cell
    attr_reader :connection, :row, :col_name, :cell

    def initialize(connection, row, col_name, cell)
      @connection, @row, @col_name, @cell = connection, row, col_name.to_sym, cell
    end

    def timestamp
      cell.timestamp
    end
    
    def value
      cell.value
    end
    
    def versions(num, max_timestamp = nil)
      row[col_name, num, max_timestamp].map {|tcell| HbaseAdapter::Cell.new(connection, row, col_name, tcell)}
    end
  end
end