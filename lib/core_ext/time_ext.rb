class Time
  def to_i64
    (self.to_f * 1000).to_i
  end
end