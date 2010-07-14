module HbaseAdapter
  class Mutation
    attr_reader :column, :value
    
    def initialize(column)
      @column = column
      @value = nil
    end
    
    def is_delete?
      false
    end
    
    def to_thrift
      Apache::Hadoop::Hbase::Thrift::Mutation.new(
        :isDelete => is_delete?,
        :column => column,
        :value => value
      )
    end
  end
  
  class UpdateMutation < Mutation
    def initialize(column, value)
      super
      @value = value
    end
  end
  
  class DeleteMutation < Mutation
    def is_delete?
      true
    end
  end
  
  class BatchMutation
    attr_reader :key, :mutations
    
    def initialize(key, &blk)
      @key = key
      @mutations = []
      
      self.instance_eval(&blk)
    end

    def update(column, value)
      @mutations << UpdateMutation.new(column, value)
    end
    
    def delete(key, column)
      @mutations << DeleteMutation.new(column)
    end
    
    def to_thrift
      Apache::Hadoop::Hbase::Thrift::BatchMutation.new(
        :row => key,
        :mutations => mutations.map {|m| m.to_thrift}
      )
    end
  end
end