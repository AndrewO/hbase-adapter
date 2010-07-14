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
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "GneralTsao"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:first_name", :value => "Andrew"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "other_stuff:favorite_color", :value => "plaid")
      ])

      @connection.client.mutateRow("test_table_users", "chris", [
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "jcobri"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:first_name", :value => "Chris"),
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "other_stuff:raison_detre", :value => "Takin' it easy for all us sinners")
      ])
    
      @connection.client.mutateRow("test_table_users", "andrew", [
        Apache::Hadoop::Hbase::Thrift::Mutation.new(:isDelete => false, :column => "info:login", :value => "AndrewO")
      ])
    
      @table = HbaseAdapter::Table.new(@connection, "test_table_users")
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

  it "accesses its cells" do
    row = HbaseAdapter::Row.new(@connection, @table, "andrew")
    cells = row.cells
    cells.should_not be_empty
  end
  
  it "handles mutations"
end