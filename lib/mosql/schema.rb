module MoSQL
  class SchemaError < StandardError; end;

  class Schema
    include MoSQL::Logging

    def initialize(map)
      @map = {}
      map.each do |dbname, db|
        @map[dbname] = { :meta => parse_meta(db[:meta]) }
        db.each do |cname, spec|
          next unless cname.is_a?(String)
          begin
            @map[dbname][cname] = parse_spec("#{dbname}.#{cname}", spec)
          rescue KeyError => e
            raise SchemaError.new("In spec for #{dbname}.#{cname}: #{e}")
          end
        end
      end

      # Lurky way to force Sequel force all timestamps to use UTC.
      Sequel.default_timezone = :utc
    end

    def to_array(lst)
      lst.map do |ent|
        col = nil
        raise SchemaError.new("Invalid ordered hash entry #{ent.inspect}") unless ent.is_a?(Hash)
        if ent[:sources].is_a?(Array) && ent[:keys].is_a?(Array) && ent[:type].is_a?(String)
          col = {
            :sources     => ent.fetch(:sources),
            :keys        => ent.fetch(:keys),
            :value       => ent[:value],
            :type        => ent.fetch(:type),
            :name        => (ent.keys - [:source, :type]).first,
            :default     => ent[:default],
            :conversions => ent[:conversions],
            :eval        => ent[:eval],
          }
        elsif ent[:source].is_a?(String) && ent[:type].is_a?(String)
          # new configuration format
          col = {
            :source      => ent.fetch(:source),
            :value       => ent[:value],
            :type        => ent.fetch(:type),
            :name        => (ent.keys - [:source, :type]).first,
            :default     => ent[:default],
            :conversions => ent[:conversions],
            :eval        => ent[:eval],
          }
        elsif ent.keys.length == 1 && ent.values.first.is_a?(String)
          col = {
            :source      => ent.first.first,
            :name        => ent.first.first,
            :type        => ent.first.last,
            :default     => ent[:default],
            :conversions => ent[:conversions],
            :eval        => ent[:eval],
          }
        elsif !ent[:value].nil? && ent[:type].is_a?(String)
          # hardcoded value format
          col = {
            :source      => nil,
            :value       => ent.fetch(:value),
            :type        => ent.fetch(:type),
            :name        => (ent.keys - [:source, :type]).first,
            :default     => ent[:default],
            :conversions => ent[:conversions],
            :eval        => ent[:eval],
          }
        else
          raise SchemaError.new("Invalid ordered hash entry #{ent.inspect}")
        end

        if !col.key?(:array_type) && /\A(.+)\s+array\z/i.match(col[:type])
          col[:array_type] = $1
        end

        col
      end
    end

    def check_columns!(ns, spec)
      seen = Set.new
      spec[:columns].each do |col|
        if seen.include?(col[:source]) && col[:value].nil? && col[:sources].nil?
          raise SchemaError.new("Duplicate source #{col[:source]} in column definition #{col[:name]} for #{ns}.")
        elsif col[:sources] && (col[:sources] - seen.to_a).size < col[:sources].size
          raise SchemaError.new("Duplicate sources #{col[:sources].join(' - ')} in column definition #{col[:name]} for #{ns}.")
        end

        seen.add(col[:source]) if col[:source]
        col[:sources].each { |source| seen.add(source) } if col[:sources]
      end
    end

    def parse_spec(ns, spec)
      out = spec.dup
      out[:columns] = to_array(spec.fetch(:columns))
      check_columns!(ns, out)
      out
    end

    def parse_meta(meta)
      meta = {} if meta.nil?
      meta[:alias] = [] unless meta.key?(:alias)
      meta[:alias] = [meta[:alias]] unless meta[:alias].is_a?(Array)
      meta[:alias] = meta[:alias].map { |r| Regexp.new(r) }
      meta
    end

    def create_schema(db, drop_table=false)
      @map.values.each do |dbspec|
        dbspec.each do |n, collection|
          next unless n.is_a?(String)
          meta = collection[:meta]
          composite_key = meta[:composite_key]
          keys = []
          log.info("Dropping and creating table '#{meta[:table]}'...") if drop_table
          db.send(drop_table ? :create_table! : :create_table?, meta[:table]) do
            collection[:columns].each do |col|
              opts = {}
              if col[:source] == '$timestamp'
                opts[:default] = Sequel.function(:now)
              end
              column col[:name], col[:type], opts

              if composite_key and composite_key.include?(col[:name])
                keys << col[:name].to_sym
              elsif not composite_key and col[:source] and col[:source].to_sym == :_id
                keys << col[:name].to_sym
              end
            end

            if meta[:auto_increment_id_pkey]
              primary_key :id
            else
              primary_key keys
            end

            if meta[:timestamps]
              column 'created_at', 'TIMESTAMP'
              column 'updated_at', 'TIMESTAMP'
            end

            if meta[:extra_props]
              type =
                case meta[:extra_props]
                when 'JSON'
                  'JSON'
                when 'JSONB'
                  'JSONB'
                else
                  'TEXT'
                end
              column '_extra_props', type
            end
          end
        end
      end
    end

    def find_db(db)
      unless @map.key?(db)
        @map[db] = @map.values.find do |spec|
          spec && spec[:meta][:alias].any? { |a| a.match(db) }
        end
      end
      @map[db]
    end

    def find_ns(ns)
      db, collection = ns.split(".", 2)
      unless spec = find_db(db)
        return nil
      end
      unless schema = spec[collection]
        log.debug("No mapping for ns: #{ns}")
        return nil
      end
      schema
    end

    def find_ns!(ns)
      schema = find_ns(ns)
      raise SchemaError.new("No mapping for namespace: #{ns}") if schema.nil?
      schema
    end

    def fetch_exists(obj, dotted)
      pieces = dotted.split(".")
      while pieces.length > 1
        key = pieces.shift
        obj = obj[key]
        return false unless obj.is_a?(Hash)
      end
      obj.has_key?(pieces.first)
    end

    def fetch_elem(obj, field_name, array_index)
      field_value = obj[field_name]
      return nil unless field_value
      element = field_value[array_index.to_i]
      if element.is_a?(Hash)
        JSON.dump(Hash[element.map { |k, primitive_value|
          [k, transform_primitive(primitive_value)]
        } ])
      else
        element
      end
    end

    def fetch_special_source(obj, source, original)
      case source
      when "$timestamp"
        Sequel.function(:now)
      when /^\$exists (.+)/
        # We need to look in the cloned original object, not in the version that
        # has had some fields deleted.
        fetch_exists(original, $1)
      when /^\$elem.([a-zA-Z_]+).(\d+)/
        # To fetch one element from array :source: $elem.0
        fetch_elem(original, $1, $2)
      else
        raise SchemaError.new("Unknown source: #{source}")
      end
    end

    def fetch_and_delete_dotted(obj, dotted)
      pieces = dotted.split(".")
      breadcrumbs = []
      while pieces.length > 1
        key = pieces.shift
        breadcrumbs << [obj, key]
        obj = obj[key]
        return nil unless obj.is_a?(Hash)
      end

      val = obj.delete(pieces.first)

      breadcrumbs.reverse.each do |value, k|
        value.delete(k) if value[k].empty?
      end

      val
    end

    def fetch_multiple_and_convert_to_hash(obj, sources, keys)
      val = {}
      sources.each_with_index do |source, idx|
        val[keys[idx]] = obj[source] if obj[source]
      end

      JSON.dump(Hash[val.map { |k, primitive_value|
        [k, transform_primitive(primitive_value)]
      } ])
    end

    def transform_primitive(v, type=nil)
      case v
      when BSON::ObjectId, Symbol
        v.to_s
      when BSON::Binary
        if type.downcase == 'uuid'
          v.to_s.unpack("H*").first
        else
          Sequel::SQL::Blob.new(v.to_s)
        end
      when BSON::DBRef
        v.object_id.to_s
      else
        v
      end
    end

    def transform(ns, obj, schema=nil)
      schema ||= find_ns!(ns)

      original = obj

      # Do a deep clone, because we're potentially going to be
      # mutating embedded objects.
      obj = BSON.deserialize(BSON.serialize(obj))

      row = []
      schema[:columns].each do |col|
        source = col[:source]
        sources = col[:sources]
        keys = col[:keys]
        value = col[:value]
        type = col[:type]
        conversions = col[:conversions]
        default_v = col[:default]

        if value
          v = value
        elsif sources && keys
          v = fetch_multiple_and_convert_to_hash(obj, sources, keys)
        elsif source.start_with?("$")
          v = fetch_special_source(obj, source, original)
        else
          v = fetch_and_delete_dotted(obj, source)
          v = eval(v) if col[:eval] && v
          case v
          when Hash
            v = JSON.dump(Hash[v.map { |k, primitive_value|
              [k, transform_primitive(primitive_value)]
            } ])
          when Array
            v = v.map { |it| transform_primitive(it) }
            if col[:array_type]
              v = Sequel.pg_array(v, col[:array_type])
            elsif type == 'BIT VARYING'
              v = v.map { |b| b ? 1 : 0 }.join
            else
              v = JSON.dump(v)
            end
          else
            v = transform_primitive(v, type)
          end
        end

        if conversions
          previous_v = v
          v = conversions[v]
          if v.nil?
            v = previous_v
          end
        end

        if !default_v.nil? && v.nil?
          row << default_v
        else
          row << v
        end
      end

      if schema[:meta][:timestamps]
        utc_time = Time.now.getutc
        row << utc_time
        row << utc_time
      end

      if schema[:meta][:extra_props]
        extra = sanitize(obj)
        row << JSON.dump(extra)
      end

      log.debug { "Transformed: #{row.inspect}" }

      row
    end

    def sanitize(value)
      # Base64-encode binary blobs from _extra_props -- they may
      # contain invalid UTF-8, which to_json will not properly encode.
      case value
      when Hash
        ret = {}
        value.each {|k, v| ret[k] = sanitize(v)}
        ret
      when Array
        value.map {|v| sanitize(v)}
      when BSON::Binary
        Base64.encode64(value.to_s)
      when Float
        # NaN is illegal in JSON. Translate into null.
        value.nan? ? nil : value
      else
        value
      end
    end

    def copy_column?(col)
      col[:source] != '$timestamp'
    end

    def all_columns(schema, copy=false)
      cols = []
      schema[:columns].each do |col|
        cols << col[:name] unless copy && !copy_column?(col)
      end
      if schema[:meta][:timestamps]
        cols << "created_at"
        cols << "updated_at"
      end
      if schema[:meta][:extra_props]
        cols << "_extra_props"
      end
      cols
    end

    def all_columns_for_copy(schema)
      all_columns(schema, true)
    end

    def copy_data(db, ns, objs)
      schema = find_ns!(ns)
      db.synchronize do |pg|
        this = all_columns_for_copy(schema)
               .map { |c| "\"#{c}\"" }.join(', ')
        sql = "COPY \"#{schema[:meta][:table]}\" (#{this}) FROM STDIN"
        pg.execute(sql)
        objs.each do |o|
          pg.put_copy_data(transform_to_copy(ns, o, schema) + "\n")
        end
        pg.put_copy_end
        begin
          pg.get_result.check
        rescue PGError => e
          db.send(:raise_error, e)
        end
      end
    end

    def quote_copy(val)
      case val
      when nil
        "\\N"
      when true
        't'
      when false
        'f'
      when Sequel::SQL::Function
        nil
      when DateTime, Time
        val.strftime("%FT%T.%6N %z")
      when Sequel::SQL::Blob
        "\\\\x" + [val].pack("h*")
      else
        val.to_s.gsub(/([\\\t\n\r])/, '\\\\\\1')
      end
    end

    def transform_to_copy(ns, row, schema=nil)
      row.map { |c| quote_copy(c) }.compact.join("\t")
    end

    def table_for_ns(ns)
      find_ns!(ns)[:meta][:table]
    end

    def all_mongo_dbs
      @map.keys
    end

    def collections_for_mongo_db(db)
      (@map[db]||{}).keys
    end

    def primary_sql_key_for_ns(ns)
      ns = find_ns!(ns)
      keys = []
      if ns[:meta][:composite_key]
        keys = ns[:meta][:composite_key]
      else
        key_fetcher = ns[:columns].find {|c| c[:source] == '_id'}
        if key_fetcher
          keys << key_fetcher[:name]
        else
          keys = [:id]
        end
      end

      return keys
    end
  end
end
