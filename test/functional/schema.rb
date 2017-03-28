require File.join(File.dirname(__FILE__), '_lib.rb')

class MoSQL::Test::Functional::SchemaTest < MoSQL::Test::Functional
  TEST_MAP = <<EOF
---
db:
  collection:
    :meta:
      :table: sqltable
    :columns:
      - _id: TEXT
      - var: INTEGER
      - arry: INTEGER ARRAY
  with_extra_props:
    :meta:
      :table: sqltable2
      :extra_props: true
    :columns:
      - _id: TEXT
  with_dotted:
    :meta:
      :table: sqltable3
      :extra_props: true
    :columns:
      - _id: TEXT
      - var_a:
        :source: vars.a
        :type: TEXT
      - var_b:
        :source: vars.b
        :type: TEXT
  with_timestamps:
    :meta:
      :table: sqltable4
      :timestamps: true
      :extra_props: true
    :columns:
      - _id: TEXT
      - var_a:
        :source: vars.a
        :type: TEXT
      - var_b:
        :source: vars.b
        :type: TEXT
  with_hardcoded_value:
    :meta:
      :table: sqltable5
    :columns:
      - _id: TEXT
      - var_a:
        :source: vars.a
        :value: harcoded_value_a
        :type: TEXT
      - var_b:
        :value: 'harcoded_value::b'
        :type: TEXT
  with_elem_source:
    :meta:
      :table: sqltable6
    :columns:
      - _id: TEXT
      - var_0:
        :source: $elem.vars.0
        :type: TEXT
  with_default:
    :meta:
      :table: sqltable7
    :columns:
      - _id: TEXT
      - var_0:
        :source: $elem.vars.0
        :default: "{ 1, 2 }"
        :type: INTEGER ARRAY
EOF

  before do
    @map = MoSQL::Schema.new(YAML.load(TEST_MAP))

    @sequel.drop_table?(:sqltable)
    @sequel.drop_table?(:sqltable2)
    @sequel.drop_table?(:sqltable3)
    @sequel.drop_table?(:sqltable4)
    @sequel.drop_table?(:sqltable5)
    @sequel.drop_table?(:sqltable6)
    @sequel.drop_table?(:sqltable7)
    @map.create_schema(@sequel)
  end

  def table; @sequel[:sqltable]; end
  def table2; @sequel[:sqltable2]; end
  def table3; @sequel[:sqltable3]; end
  def table4; @sequel[:sqltable4]; end
  def table5; @sequel[:sqltable5]; end
  def table6; @sequel[:sqltable6]; end
  def table7; @sequel[:sqltable7]; end

  it 'Creates the tables with the right columns' do
    assert_equal(Set.new([:_id, :var, :arry]),
                 Set.new(table.columns))
    assert_equal(Set.new([:_id, :_extra_props]),
                 Set.new(table2.columns))
  end

  it 'Can COPY data' do
    objects = [
               {'_id' => "a", 'var' => 0},
               {'_id' => "b", 'var' => 1, 'arry' => "{1, 2, 3}"},
               {'_id' => "c"},
               {'_id' => "d", 'other_var' => "hello"}
              ]
    @map.copy_data(@sequel, 'db.collection', objects.map { |o| @map.transform('db.collection', o) } )
    assert_equal(4, table.count)
    rows = table.select.sort_by { |r| r[:_id] }
    assert_equal(%w[a b c d], rows.map { |r| r[:_id] })
    assert_equal(nil, rows[2][:var])
    assert_equal(nil, rows[3][:var])
    assert_equal([1 ,2, 3], rows[1][:arry])
  end

  it 'Can COPY dotted data' do
    objects = [
               {'_id' => "a", 'vars' => {'a' => 1, 'b' => 2}},
               {'_id' => "b", 'vars' => {}},
               {'_id' => "c", 'vars' => {'a' => 2, 'c' => 6}},
               {'_id' => "d", 'vars' => {'a' => 1, 'c' => 7}, 'extra' => 'moo'}
              ]
    @map.copy_data(@sequel, 'db.with_dotted', objects.map { |o| @map.transform('db.with_dotted', o) } )
    assert_equal(4, table3.count)
    o = table3.first(:_id => 'a')
    assert_equal("1", o[:var_a])
    assert_equal("2", o[:var_b])

    o = table3.first(:_id => 'b')
    assert_equal({}, JSON.parse(o[:_extra_props]))

    o = table3.first(:_id => 'c')
    assert_equal({'vars' => { 'c' => 6} }, JSON.parse(o[:_extra_props]))

    o = table3.first(:_id => 'd')
    assert_equal({'vars' => { 'c' => 7}, 'extra' => 'moo' }, JSON.parse(o[:_extra_props]))
    assert_equal(nil, o[:var_b])
  end

  it 'Can COPY BSON::ObjectIDs' do
    o = {'_id' => BSON::ObjectId.new, 'var' => 0}
    @map.copy_data(@sequel, 'db.collection', [ @map.transform('db.collection', o)] )
    assert_equal(o['_id'].to_s, table.select.first[:_id])
  end

  it 'Can ASSIGN timestamps' do
    objects = [
               {'_id' => "a", 'vars' => {'a' => 1, 'b' => 2}},
               {'_id' => "b", 'vars' => {}},
               {'_id' => "c", 'vars' => {'a' => 2, 'c' => 6}},
              ]
    @map.copy_data(@sequel, 'db.with_timestamps', objects.map { |o| @map.transform('db.with_timestamps', o) } )
    assert_equal(3, table4.count)
    o = table4.first(:_id => 'a')
    assert_equal("1", o[:var_a])
    assert_equal("2", o[:var_b])
    assert_equal(Time, o[:created_at].class)
    assert_equal(Time, o[:updated_at].class)

    o = table4.first(:_id => 'b')
    assert_equal({}, JSON.parse(o[:_extra_props]))
    assert_equal(Time, o[:created_at].class)
    assert_equal(Time, o[:updated_at].class)

    o = table4.first(:_id => 'c')
    assert_equal({'vars' => { 'c' => 6} }, JSON.parse(o[:_extra_props]))
    assert_equal(Time, o[:created_at].class)
    assert_equal(Time, o[:updated_at].class)
  end


  it 'Can ASSIGN hardcoded values' do
    objects = [
               {'_id' => "a", 'vars' => {'a' => 1, 'b' => 2}},
               {'_id' => "b", 'vars' => {}},
               {'_id' => "c", 'vars' => {'a' => 2, 'c' => 6}},
              ]
    @map.copy_data(@sequel, 'db.with_hardcoded_value', objects.map { |o| @map.transform('db.with_hardcoded_value', o) } )
    assert_equal(3, table5.count)
    o = table5.first(:_id => 'a')
    assert_equal('harcoded_value_a', o[:var_a])
    assert_equal('harcoded_value::b', o[:var_b])

    o = table5.first(:_id => 'b')
    assert_equal('harcoded_value_a', o[:var_a])
    assert_equal('harcoded_value::b', o[:var_b])

    o = table5.first(:_id => 'c')
    assert_equal('harcoded_value_a', o[:var_a])
    assert_equal('harcoded_value::b', o[:var_b])
  end

  it 'Can FETCH elements by array index' do
    objects = [
      {'_id' => "a", 'vars' => ["hello", "world"]},
      {'_id' => "b", 'vars' => ["life", "is", "good"]}
    ]

    @map.copy_data(@sequel, 'db.with_elem_source', objects.map { |o| @map.transform('db.with_elem_source', o) } )
    assert_equal(2, table6.count)
    o = table6.first(:_id => 'a')
    assert_equal('hello', o[:var_0])

    o = table6.first(:_id => 'b')
    assert_equal('life', o[:var_0])
  end

  it 'Can FETCH elements by array index' do
    objects = [
      {'_id' => "a", 'vars' => ["{1}"]},
      {'_id' => "b", 'vars' => []}
    ]

    @map.copy_data(@sequel, 'db.with_default', objects.map { |o| @map.transform('db.with_default', o) } )
    assert_equal(2, table7.count)
    o = table7.first(:_id => 'a')
    assert_equal([1], o[:var_0])

    o = table7.first(:_id => 'b')
    assert_equal([1, 2], o[:var_0])
  end

  it 'Can transform BSON::ObjectIDs' do
    o = {'_id' => BSON::ObjectId.new, 'var' => 0}
    row = @map.transform('db.collection', o)
    table.insert(row)
    assert_equal(o['_id'].to_s, table.select.first[:_id])
  end

  describe 'special fields' do
  SPECIAL_MAP = <<EOF
---
db:
  collection:
    :meta:
      :table: special
    :columns:
      - _id: TEXT
      - mosql_updated:
        :source: $timestamp
        :type: timestamp
EOF

    before do
      @specialmap = MoSQL::Schema.new(YAML.load(SPECIAL_MAP))

      @sequel.drop_table?(:special)
      @specialmap.create_schema(@sequel)
    end

    it 'sets a default on the column' do
      @sequel[:special].insert({_id: 'a'})
      row = @sequel[:special].select.first
      assert_instance_of(Time, row[:mosql_updated])
    end

    it 'Can populate $timestamp on COPY' do
      objects = [
                 {'_id' => "a"},
                 {'_id' => "b"}
                ]
      before = @sequel.select(Sequel.function(:NOW)).first[:now]
      @specialmap.copy_data(@sequel, 'db.collection',
                            objects.map { |o| @specialmap.transform('db.collection', o) } )
      after = @sequel.select(Sequel.function(:NOW)).first[:now]
      rows = @sequel[:special].select.sort_by { |r| r[:_id] }

      assert_instance_of(Time, rows[0][:mosql_updated])
      assert_operator(rows[0][:mosql_updated], :>, before)
      assert_operator(rows[0][:mosql_updated], :<, after)
    end
  end
end
