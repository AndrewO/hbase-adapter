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
    
    # Causes HBase to freak out with an ArrayIndexOutOfBoundsException.  Have no idea if it's my fault, but ignoring for now
    # def incr!(incr_val = 1)
    #   connection.client.atomicIncrement(row.table.name.to_s, row.key.to_s, col_name.to_s, incr_val)
    # end
    
    # TODO: easy version access
  end
end