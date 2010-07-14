require File.dirname(__FILE__) + "/spec_helper"

describe "HbaseAdapter::Table" do
  before do
    @connection = HbaseAdapter::Connection.new(:host => HBASE_CONNECTION_PARAMS[0], :port => HBASE_CONNECTION_PARAMS[1])
    @table = HbaseAdapter::Table.new(@connection, "users")
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
end