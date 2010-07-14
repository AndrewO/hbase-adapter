require File.dirname(__FILE__) + "/spec_helper"

describe "HbaseAdapter::Row" do
  before do
    begin
      @connection = HbaseAdapter::Connection.new(:host => HBASE_CONNECTION_PARAMS[0], :port => HBASE_CONNECTION_PARAMS[1])
    
      @connection.client.createTable("test_table_users", [
        Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new(:name => "info"), 
        Apache::Hadoop::Hbase::Thrift::ColumnDescriptor.new(:name => "other_stuff")
      ])
    
      @connection.client.mutateRow("test_table_users", "andrew", [
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "alobri"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:first_name", :value => "Andrew"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "other_stuff:favorite_color", :value => "plaid")
      ])

      @connection.client.mutateRow("test_table_users", "chris", [
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "jcobri"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:first_name", :value => "Chris"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "other_stuff:raison_detre", :value => "Takin' it easy for all us sinners")
      ])
      
      @connection.client.mutateRow("test_table_users", "andrew", [
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "GneralTsao")
      ])
    
      @time_between = Time.now
      sleep 0.0001

      @connection.client.mutateRow("test_table_users", "andrew", [
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "AndrewO")
      ])
    
      @table = HbaseAdapter::Table.new(@connection, "test_table_users")
      
      @row = HbaseAdapter::Row.new(@connection, @table, "andrew")
    rescue HbaseAdapter::TableAlreadyExists
      @connection.client.disableTable("test_table_users")
      @connection.client.deleteTable("test_table_users")
    end
  end
  
  after do
    begin
      @connection.client.disableTable("test_table_users")
      @connection.client.deleteTable("test_table_users")
    rescue HbaseAdapter::IOError
    end
  end

  # This API might change.  I [] and cells may be combined and I don't like how the hash is required in [].
  it "accesses its cells" do
    cells = @row.cells
    cells.should_not be_empty
  end
  
  it "accesses its cells with a timestamp" do
    cells = @row.cells(:timestamp => @time_between)
    cells.should_not be_empty
  end
  
  it "accesses particular columns from its cells" do
    cells = @row.cells("info:login", "other_stuff:favorite_color")
    cells.should_not be_empty
  end

  it "accesses particular columns from its cells with a timestamp" do
    cells = @row.cells("info:login", "other_stuff:favorite_color", :timestamp => @time_between)
    cells.should_not be_empty
  end
  
  it "accesses single columns from its cells" do
    cell = @row["info:login"]
    cell.should_not be_nil
  end
  
  it "accesses a certain number of versions single columns from its cells" do
    cells = @row["info:login", {:num_versions => 3}]
    cells.should_not be_empty
  end
  
  it "accesses a certain number of versions single columns from its cells with a timestamp" do
    cells = @row["info:login", {:num_versions => 3, :timestamp => @time_between}]
    cells.should_not be_empty
  end

  it "handles mutations"
end