require File.dirname(__FILE__) + "/spec_helper"

describe "HbaseAdapter::ColumnFamily" do
  it "allows getting and setting of column family data" do
    cf = HbaseAdapter::ColumnFamily.new(:name => "foo")
    
    cf.name.should == "foo"
    cf.max_versions.should_not be_nil
    # TODO: test others when implemented
  end
end