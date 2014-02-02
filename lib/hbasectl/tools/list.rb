include Java
require 'hbasectl'
require 'hbasectl/admin'
require 'hbasectl/tools'

module HbaseCtl

  module Tools

    # struct for keeping the details of a region
    RegionStruct = Struct.new(:table, :region_name, :server, :offline, :compaction, :num_store_file, :size_of_store_file, :num_requests)
    TableStruct = Struct.new(:table, :families, :compaction, :read_only, :enabled, :available )

    class List

      attr_reader :hb_admin, :cluster_status

      def initialize(opts={})
        @command = self.class.to_s
        # admin object for zookeeper and other hbase operations
        @hb_admin = HbaseCtl::Admin.new()
        @cluster_status = @hb_admin.admin.getClusterStatus()
      end

      # list the regions of a table and pretty print
      # @params
      #  - table_name
      # @returns
      #  - none

      def list_regions(table_name)
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# invalid table #{table_name}") if table_name.nil? || table_name.empty?
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# table #{table_name} doesn't exist") unless @hb_admin.table_exist?(table_name)

        pairs = @hb_admin.get_regions(table_name)
        # to cache the server_load objects
        server_loads = Hash.new

        # to cache the region_load objects
        region_loads = Hash.new

        pairs.each do |pair|
          h_region = pair.first
          h_server = pair.second
          h_region_name = h_region.getRegionNameAsString
          h_region_encoded = h_region.getEncodedName
          h_server_name = h_server.getHostname
          compaction_state = case @hb_admin.get_compaction_state(h_region_encoded).first[h_region_encoded]   # this is an array of hashes with region_name as key
                               when HbaseCtl::Constants::COMPACTION_STATE_MAJOR
                                 "MAJOR"
                               when HbaseCtl::Constants::COMPACTION_STATE_MINOR
                                 "MINOR"
                               when HbaseCtl::Constants::COMPACTION_STATE_MAJOR_AND_MINOR
                                 "MAJOR_AND_MINOR"
                               when HbaseCtl::Constants::COMPACTION_STATE_NONE
                                 "NONE"
                               else
                                 "UNKNOWN"
                             end

          # get HServerLoad for a server
          # if we have cached the value get that else get it from cluster_status
          server_load = server_loads.has_key?(h_server_name) ? server_loads[h_server_name] : @cluster_status.getLoad(h_server)
          next if server_load.nil?
          server_loads[h_server_name] = server_load unless server_loads.has_key?(h_server_name)


          # get RegionsLoad
          region_load_map = region_loads.has_key?(h_server_name) ? region_loads[h_server_name] : server_load.getRegionsLoad()
          region_loads[h_server_name] = region_load_map unless region_loads.has_key?(h_server_name)

          region_load_map.each_pair do |region_byte,region_load|
            map = @hb_admin.get_region(region_byte)
            next if map.nil? || map.first.nil?
            encoded_region_in_server = map.first.getEncodedName
            if h_region_encoded == encoded_region_in_server
              rs = RegionStruct.new(table_name, h_region_encoded, h_server_name, h_region.is_offline? , compaction_state , region_load.getStorefiles, region_load.getStorefileSizeMB, region_load.getRequestsCount)
              puts rs

            end

          end  # end region_load_map iterator


        end # end HregionInfo pair iterator

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
      end # method end

      # list the status of a region
      # @params
      #  - region_name
      # @returns
      #  - none

      def list_region(region_name)

        pair = @hb_admin.get_region(region_name)
        if pair.nil?
          HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}: unable to get region #{region_name}")
          exit
        end



        h_region = pair.first
        h_server = pair.second
        h_region_name = h_region.getRegionNameAsString
        h_region_encoded = h_region.getEncodedName
        table_name = h_region.getTableNameAsString
        h_server_name = h_server.getHostname
        compaction_state = case @hb_admin.get_compaction_state(h_region_encoded).first[h_region_encoded]   # this is an array of hashes with region_name as key
                             when HbaseCtl::Constants::COMPACTION_STATE_MAJOR
                               "MAJOR"
                             when HbaseCtl::Constants::COMPACTION_STATE_MINOR
                               "MINOR"
                             when HbaseCtl::Constants::COMPACTION_STATE_MAJOR_AND_MINOR
                               "MAJOR_AND_MINOR"
                             when HbaseCtl::Constants::COMPACTION_STATE_NONE
                               "NONE"
                            else
                              "UNKNOWN"
                            end

        # get HServerLoad for a server

        server_load = @cluster_status.getLoad(h_server)
        if server_load.nil?
          HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}: unable to get the status of #{h_server}")
          exit
        end

        # get RegionsLoad
        region_load_map =  server_load.getRegionsLoad()

        region_load_map.each_pair do |region_byte,region_load|
          # get the hregion_info and compare the encoded regionname against the region returned by the RegionLoad
          map = @hb_admin.get_region(region_byte)
          next if map.nil? || map.first.nil?
          encoded_region_in_server = map.first.getEncodedName
          if h_region_encoded== encoded_region_in_server
            rs = RegionStruct.new(table_name, h_region_encoded, h_server_name, h_region.is_offline? , compaction_state , region_load.getStorefiles, region_load.getStorefileSizeMB, region_load.getRequestsCount)
            puts rs

          end

        end  # end region_load_map iterator

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e
      end # method end

      # show status of a table
      # @params
      #  - table_name
      # @returns
      #  - none

      def list_table(table_name)

        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# invalid table #{table_name}") if table_name.nil? || table_name.empty?
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# table #{table_name} doesn't exist") unless @hb_admin.table_exist?(table_name)
        table_name_byte = table_name.to_java_bytes
        htable_descriptor = @hb_admin.connection.getHTableDescriptor(table_name_byte)

        if ! htable_descriptor.nil?
          column_families = htable_descriptor.getColumnFamilies
          column_family_names = column_families.map(&:getNameAsString).join(",")

          table_available = @hb_admin.connection.isTableAvailable(table_name_byte)
          table_enabled   = @hb_admin.connection.isTableEnabled(table_name_byte)
          read_only       = htable_descriptor.isReadOnly
          compaction_state = case @hb_admin.get_compaction_state(table_name).first[table_name]   # this is an array of hashes with region_name as key
                             when HbaseCtl::Constants::COMPACTION_STATE_MAJOR
                                 "MAJOR"
                             when HbaseCtl::Constants::COMPACTION_STATE_MINOR
                                 "MINOR"
                             when HbaseCtl::Constants::COMPACTION_STATE_MAJOR_AND_MINOR
                                 "MAJOR_AND_MINOR"
                             when HbaseCtl::Constants::COMPACTION_STATE_NONE
                                 "NONE"
                             else
                                 "UNKNOWN"
                             end

          ts = TableStruct.new(table_name, column_family_names, compaction_state, read_only, table_enabled, table_available)
          puts ts

        else
          HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# unabled to get status of table #{table_name}")
        end

      rescue Exception => e
        HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#{__method__}:# failed: #{e.class} : #{e.message} : #{e.backtrace.inspect}")
        raise e

      end #method end

    end # class end
  end # module Tools end
end # module HbaseCtl end

