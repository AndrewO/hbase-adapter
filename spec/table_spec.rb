require File.dirname(__FILE__) + "/spec_helper"

describe "HbaseAdapter::Table" do
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
  
  it "can enable or disable a table and get its status" do
    @table.should be_enabled
    @table.disable!
    @table.should_not be_enabled
    @table.enable!
    @table.should be_enabled
  end
  
  it "can compact tables" do
    lambda { @table.compact! }.should_not raise_exception
  end
  
  it "can major compact tables" do
    lambda { @table.major_compact! }.should_not raise_exception
  end
  
  it "gets a table's columns" do
    @table.columns.should_not be_nil
  end
  
  it "gets a table's regions" do
    @table.regions.should_not be_nil
  end
  
  it "gets rows by key" do
    row = @table["andrew"]
    row.should_not be_nil
  end
  
  it "deletes a row" do
    @table.delete_row!("andrew")
    @table["andrew"].cells.should be_nil
  end
  
  it "deletes a row by timestamp" do
    @table.delete_row!("andrew", :timestamp => @time_between)
    @table["andrew"].cells.should_not be_nil
  end
  
  it "bulk mutates a row" do
    @table.mutate! do
      batch_mutation("andrew") do
        delete "other_stuff:favorite_color"
        update "info:login", "andrewo"
      end
      
      batch_mutation("chris") do
        update "other_stuff:favorite_color", "clear"
      end
    end
    
    @table["andrew"]["info:login"].value.should == "andrewo"
    @table["andrew"]["other_stuff:favorite_color"].should be_nil
    @table["chris"]["other_stuff:favorite_color"].value.should == "clear"
  end
  
  it "bulk mutates a row by timestamp" do
    @table.mutate!(:timestamp => Time.now - 24 * 60 * 60) do
      batch_mutation("andrew") do
        delete "other_stuff:favorite_color"
        update "info:login", "AndrewO"
      end
      
      batch_mutation("chris") do
        update "other_stuff:favorite_color", "clear"
      end
    end
    
    @table["andrew"]["info:login"].value.should == "AndrewO"
    @table["andrew"]["other_stuff:favorite_color"].should_not be_nil
    @table["chris"]["other_stuff:favorite_color"].value.should == "clear"
  end
end