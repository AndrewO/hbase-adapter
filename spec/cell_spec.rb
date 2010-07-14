require File.dirname(__FILE__) + "/spec_helper"

describe "HbaseAdapter::Cell" do
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
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "other_stuff:favorite_color", :value => "plaid"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "other_stuff:favorite_number", :value => "9")
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
      @cell = @row["other_stuff:favorite_color"]
    rescue HbaseAdapter::TableAlreadyExists
      @connection.client.disableTable("test_table_users")
      @connection.client.deleteTable("test_table_users")
      
      retry
    end
  end
  
  after do
    begin
      @connection.client.disableTable("test_table_users")
      @connection.client.deleteTable("test_table_users")
    rescue HbaseAdapter::IOError
    end
  end
  
  it "gives access to its value and timestamp" do
    cell = @row["other_stuff:favorite_color"]

    cell.value.should == "plaid"
    cell.timestamp.should_not be_nil
  end
  
  it "can get previous versions of itself"
  
  it "it can atomically increment" # do
   #    cell = @row["other_stuff:favorite_number"]
   #    
   #    cell.value.to_i.should == 9
   #    cell.incr!(10)
   #    cell.value.to_i.should == 19
   #  end
end