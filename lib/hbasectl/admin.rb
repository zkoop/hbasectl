include Java
java_import 'java.util.ArrayList'
java_import 'org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher'
java_import 'org.apache.hadoop.hbase.HBaseConfiguration'
java_import 'org.apache.hadoop.hbase.client.HBaseAdmin'
java_import 'org.apache.zookeeper.ZooKeeper'
java_import 'org.apache.zookeeper.data.Stat'
java_import 'org.apache.hadoop.hbase.catalog.CatalogTracker'
java_import 'org.apache.hadoop.hbase.HConstants'
java_import 'org.apache.hadoop.hbase.client.Scan'
java_import 'org.apache.hadoop.hbase.filter.PrefixFilter'
java_import 'org.apache.hadoop.hbase.client.HTable'
java_import 'org.apache.hadoop.hbase.util.Bytes'
java_import 'org.apache.hadoop.hbase.util.Writables'
java_import 'org.apache.hadoop.hbase.catalog.MetaReader'
module HbaseCtl
  
  class Admin

    # class for Abortable interface.
    # This is needed if we need to use org.apache.hadoop.hbase.zookeeper.ZKUtil
    # ZKUtil has some functions to interact with zookeeper for e.g: creating nested znodes
    class Abortable
      java_implements 'Abortable'

    end

    attr_reader :configuration, :admin, :connection,  :zk, :zk_stat, :znode_base, :zk_watcher, :abortable, :ct


    def initialize(opts={})

      @configuration = HBaseConfiguration.create
      @admin = HBaseAdmin.new(@configuration)
      @connection = @admin.getConnection()
      #@zk_wrapper = @connection.getZooKeeperWatcher() # this is deprecated 
      @abortable = Abortable.new()
      @zk_watcher=ZooKeeperWatcher.new(@configuration,"HbaseCtlWatcher",@abortable)
      @zk=ZooKeeper.new(@zk_watcher.getQuorum, 30000, @zk_watcher)
      @zk_stat=Stat.new()
      @znode_base=File.join("/admin",@zk_watcher.baseZNode)
      #create a catalog tracker
      @ct = CatalogTracker.new(@zk_watcher,@configuration,@abortable)

    end

    # check whether an znode exist
    # @params 
    #   - path of znode
    # @returns
    #   - Boolean
    def znode_exists?(znode)
      return false if HbaseCtl.dryrun
      stat = @zk.exists(znode, true)
      stat.class == Java::OrgApacheZookeeperData::Stat ? true : false
      rescue org.apache.zookeeper.KeeperException, java.lang.InterruptedException => e
        HbaseCtl.log("#{HbseCtl.hostname}:#{self.class}:# #{e.class} #{e.message} #{e.backtrace}")
        raise e
    end

    # create a znode
    # @params
    #   - path of znode
    #   - additional options
    # @returns
    #   - path of znode created
    def create_znode(znode,opts={})
      return znode if HbaseCtl.dryrun
      # if znode is empty raise exception
      raise HbaseCtl::ZookeeperException.new(__method__,100, "#{HbaseCtl.hostname} : create_znode called without a node") if znode.nil? ||  znode.empty?

      # any data to be stored for znode
      node_info = opts.delete(:info) || ""

      # type of znode default 3
      create_mode_flag = opts.delete(:mode) || HbaseCtl::Constants::MODE_EPHEMERAL_SEQUENTIAL
      cmode=org.apache.zookeeper.CreateMode.fromFlag(create_mode_flag) # default flag EPHEMERAL_SEQUENTIAL


      # permission on the node, default open to all
      zoo_perm = opts.delete(:perm) || HbaseCtl::Constants::ZOO_PERM_ALL
      acl=org.apache.zookeeper.data.ACL.new()
      acl.setPerms(zoo_perm)
      id=org.apache.zookeeper.data.Id.new('world','anyone')
      acl.setId(id)
      list=ArrayList.new()
      list.add(acl)

      nested = opts.delete(:nested) || false
      if nested
        org.apache.hadoop.hbase.zookeeper.ZKUtil.createWithParents(@zk_watcher,znode) # this method sets acl only for the user who creates the path
        if znode_exists?(znode)
          return znode
        else
          raise  HbaseCtl::ZookeeperException.new(__method__,100, "#{HbaseCtl.hostname} : creating nested path failed")
        end
      else
        zk_path = @zk.create(znode,node_info.to_java_bytes,list,cmode)
      end

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message}")
        raise e
    end

    # fetch the znode with its associated data
    # @params
    #   - path of znode
    #   - additional options
    # @returns
    #   - two element array of converted node data and stat object
    
    def get_znode(znode,opts={})
      return ["",org.apache.zookeeper.data.Stat.new()] if HbaseCtl.dryrun
      return [nil,nil] if  znode.empty? or znode.nil?
      zk_stat = org.apache.zookeeper.data.Stat.new()
      zk_data = @zk.getData(znode,true,zk_stat)
      return [zk_data.to_a.pack('c*'), zk_stat]

      rescue Exception => e
       HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed on #{znode} : #{e.class} : #{e.message}")
       raise e
    end

    # get all the children of an znode
    # @params
    #   - path of znode
    #   - watcher object
    #   - additional options
    # @returns
    #   - array of znodes
    def get_children(znode,watch,opts={})
      return [] if HbaseCtl.dryrun
      nodes = @zk.getChildren(znode,watch)
      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
    end

    # set data of an znode
    # @params
    #   - path of znode
    #   - data
    #   - version
    #   - additional options
    # @returns
    #   - stat object
    def set_znode(znode,data,version,opts={})
      return nil if znode.nil? or znode.empty? or HbaseCtl.dryrun
      case data
      when String
        data = data.to_java_bytes
      else
        data = data.to_s.to_java_bytes
      end

      stat = @zk.setData(znode,data,version)

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
      
    
    end

    # delete an znode
    # @params
    #   - path of znode
    #   - version of znode
    # @returns
    #   - none
    def delete_znode(path,version)
      return true if HbaseCtl.dryrun
      @zk.delete(path,version)
      return znode_exists?(path) ? false : true
      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
    end

    
    # get the state of a region or table
    # @params
    #   - region or table name
    # @returns
    #   - Array of hashes containing region and compaction state as key/value pair. State is one of MAJOR, MINOR, MAJOR_MINOR, NONE

    def get_compaction_state(region_or_table)

      status = Array.new
      case region_or_table
      when Array
        region_or_table.each do |region|
          if region.nil? || region.empty? || HbaseCtl.dryrun
            val = { region => HbaseCtl::Constants::COMPACTION_STATE_NONE }
            status <<   val
            yield [val] if block_given?
            next
          else
            val =  { region => @admin.getCompactionState(region) }
            status <<  val
            yield [val] if block_given?
          end
        end
      when String
        if HbaseCtl.dryrun
          status << { region_or_table => HbaseCtl::Constants::COMPACTION_STATE_NONE }
        else
          status << { region_or_table => @admin.getCompactionState(region_or_table) }
        end
      else
        if HbaseCtl.dryrun
          status << { String.from_java_bytes(region_or_table) => HbaseCtl::Constants::COMPACTION_STATE_NONE }
        else
          status << { String.from_java_bytes(region_or_table) => HbaseCtl::Constants::COMPACTION_STATE_NONE }
        end
      end 
      return status

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
    end

    # get the split state of a region
    # @params
    #  - region or table name
    # @returns
    #  - array of hash containing status of region. status is one of SPLIT, SPLIT_NONE, SPLITTING

    def get_split_state(region_or_table)

      status = Array.new
      case region_or_table
      when Array
        region_or_table.each do |region|
          if region.nil? || region.empty? || HbaseCtl.dryrun
            val = { region => HbaseCtl::Constants::RS_REGION_SPLIT }
            status <<  val
            yield val if block_given?
            next
          else
            val = {region => split_state(region) }
            status <<  val
            yield val if block_given?
          end
        end
      when String
        if HbaseCtl.dryrun
          status << { region_or_table => HbaseCtl::Constants::RS_REGION_SPLIT }
        else
          status << { region_or_Table => split_state(region_or_table) }
        end
      else
        if HbaseCtl.dryrun
          status << { region_or_table => HbaseCtl::Constants::RS_REGION_SPLIT }
        else
          status << { region_or_table => split_state(region_or_table) }
        end
      end
      return status

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
    end

    # scan the .META. table and analyze the serialized HRegionInfo
    # @params
    #  - region_or_table
    # @returns
    #  - status. One of SPLIT, SPLIT_NONE, SPLITTING

    def split_state(region_or_table)
      region_bytes = region_or_table.to_java_bytes

      # get the complete region name
      pair = get_region(region_or_table)
      unless pair.nil?
        full_region_name = pair.first.getRegionNameAsString #HRegionInfo
        region_bytes = full_region_name.to_java_bytes
      end



      # column family and daughter region qualifiers
      family = HbaseCtl::Constants::CATALOG_FAMILY
      columns = [ HbaseCtl::Constants::REGIONINFO_QUALIFIER, HbaseCtl::Constants::SPLITA_QUALIFIER, HbaseCtl::Constants::SPLITB_QUALIFIER]

      # row to start our scan
      startrow = region_bytes

      #get the meta table
      meta = HTable.new(@configuration, '.META.')

      # create a scan object and add regioninfo,splitA,splitB qualifiers
      scan = Scan.new(startrow)
      columns.each do |column|
        scan.addColumn(family,column)
      end

      # prefixfilter to filter only our region
      filter = PrefixFilter.new(startrow)
      scan.setFilter(filter)

      # get the results and iterator
      result_scanner = meta.getScanner(scan)
      iterator = result_scanner.iterator
      unless iterator.hasNext
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region #{region_or_table} not found in .META.")
        return HbaseCtl::Constants::RS_REGION_SPLIT
      end

      # we are interested in only one row
      row = iterator.next
      #get all the qualifiers for that row
      row.list.each do |kv|
        column_family = String.from_java_bytes(kv.getFamily)
        column_qualifier = Bytes::toStringBinary(kv.getQualifier)
        column = "#{column_family}:#{column_qualifier}"

        #get the serialized HRegionInfo
        hregion_info = Writables.getHRegionInfoOrNull(kv.getValue)

        case column
        when HbaseCtl::Constants::REGIONINFO_FAMILY

          if hregion_info.nil?
            HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region: #{region_or_table} column: #{column} no serialized HRegionInfo")
            return   HbaseCtl::Constants::RS_REGION_SPLIT_NONE
          else
            if hregion_info.is_offline?
              HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region: #{region_or_table}  is offline")
            end
            if hregion_info.is_split?
              HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region: #{region_or_table} column: #{column} status: split in progress")
              return   HbaseCtl::Constants::RS_REGION_SPLITTING
            else
              HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region: #{region_or_table} column: #{column} status: split not started")
              return  HbaseCtl::Constants::RS_REGION_SPLIT_NONE
            end
          end
        when HbaseCtl::Constants::SPLITA_FAMILY
          # there is a daughter region created for splitA
          # most probably this block will not be executed as the previous block might capture the status of split

          if hregion_info.nil?
            HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region: #{region_or_table} column: #{column} no serialized HRegionInfo")
          else
            HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region #{region_or_table} column: #{column} daughter region found in .META. regionId: #{hregion_info.getRegionId} status: split in progress")
            return   HbaseCtl::Constants::RS_REGION_SPLITTING
          end
        when HbaseCtl::Constants::SPLITB_FAMILY
          # there is a daughter region created for splitB
          if hregion_info.nil?
            HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region: #{region_or_table} column: #{column} no serialized HRegionInfo")
          else
            HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# region #{region_or_table} column: #{column} daughter region found in .META. regionId: #{hregion_info.getRegionId} status: split in progress")
            return   HbaseCtl::Constants::RS_REGION_SPLITTING
          end

        end # case end
      end
    rescue Exception => e
      HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
      raise e
    end

    # splits a region or table
    # @params
    #  - region or table name
    # @returns
    #  - none
    def split(region_or_table)
      return true if HbaseCtl.dryrun
      return false if region_or_table.nil? || region_or_table.empty?

      @admin.split(region_or_table)

    rescue Exception => e
      HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
      raise e
    end

    # compacts a region or table
    # @params 
    #   - region or table name
    # @returns
    #   - none
    def compact(region_or_table,opts={})

      return true if HbaseCtl.dryrun
      opts[:type] = "major" unless opts.has_key?(:type)
      return false if region_or_table.nil? || region_or_table.empty?

      if opts[:type] == "major"
        @admin.majorCompact(region_or_table)
      else
        @admin.compact(region_or_table)
      end

 
      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
    
    end

    def table_exist?(table_name)
      return true if HbaseCtl.dryrun
      @admin.tableExists(table_name)
      rescue java.io.IOException => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__} failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
    end

    # get the region as a pair of HRegionInfo and ServerName class
    # @input 
    #   - region name
    # @output
    #   - Pair(HRegionInfo, ServerName )
    #
    def get_region(region)
      return nil if region.nil? || region.empty?
      if region.kind_of? String
        pair = @admin.getRegion(region.to_java_bytes,@ct)
      else
        pair = @admin.getRegion(region,@ct)
      end

    rescue Exception => e
      HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
      raise e

    end

    # get the list of regioninfos and server
    # @params
    #  - table name
    # @returns
    #  - list of regioninfos and server
    def get_regions(table_name)
      return nil if table_name.nil? || table_name.empty?
      # pairs will be ArraList of Pair<HRegionInfo,ServerName>
      pairs = MetaReader.getTableRegionsAndLocations(@ct, table_name)


    rescue Exception => e
      HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
      raise e
    end

  end

end
