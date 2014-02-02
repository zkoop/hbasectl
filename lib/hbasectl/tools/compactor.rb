
include Java
require 'yaml'
require 'yaml/store'
require 'monitor'
require 'hbasectl'
require 'hbasectl/admin'
require 'hbasectl/tools'
java_import 'org.apache.zookeeper.KeeperException'
java_import 'java.lang.InterruptedException'
java_import 'java.lang.IllegalArgumentException'

module HbaseCtl

  module Tools

    class InputError < StandardError

    end

    class ArgumentError < RuntimeError
  
    end

    class ArgumentMissing < StandardError

      attr_reader :arg, :err

      def initialize(arg,err='')
        message = 'Error: '
        if (arg.is_a? String)
          message <<  "Argument missing : [#{arg}]"
        elsif (arg.is_a? Hash)
          message << "Arguments missing"
          message << arg.keys.join(',')
        elsif (arg.is_a? Array )
          message << "Arguments missing #{arg.join(',')} "
        end

        message << err unless err.empty? || err.nil?
        super message
      end

    end # ArgumentMissing end

    # Implements the Java Watcher interface
    # Use this class for callback when waiting for a lock
    class Watch
      java_implements 'Watcher'
      
      def initialize(lock,cond)
        @lock = lock
        @cond = cond
      end


      def process(event)
        @lock.synchronize do
          HbaseCtl.log("#{HbaseCtl.hostname}:#{self.class}:#  Processing zookeeper watcher event : node #{event.getPath}")
          @cond.signal
        end
      end
    end

    

    # main class for Compaction. 
    class Compactor < Base

      attr_reader :command

      # input file name with the list of regions to compact
      attr_reader :input


      # queue for progress
      attr_reader  :progress 

      # entire yaml input
      attr_reader  :seed

      # number of threads to run
      attr_accessor :concurrency 

      # constructor
      def initialize(opts = {} )

        # set reasonable defaults
        @command = self.class.to_s
        @concurrency = opts.delete(:concurrency) || 5 
        @cluster = opts.delete(:cluster)  
        @dry_run  = opts.delete(:dryrun) || false
        HbaseCtl.dryrun = @dry_run
        @work_queue_percent = opts.delete(:percent) || 5 # percentage of regions to compact in one run. Default: 5. No more than 5% of regions should not be compacted in one shot.
        @input = opts.delete(:file) 
        raise InputError.new("#@input not found") unless File.exist?(@input)
        @seed = load_input(@input)

        # instance variables for keeping track of progress 
        @progress = Array.new

        # admin object for zookeeper and other hbase operations
        @admin = HbaseCtl::Admin.new()

        # base znode for hbasectl
        @znode_base=File.join("/admin",@admin.zk_watcher.baseZNode,"hbasectl") #/admin/hbase/cluster
        
        # znode path for compaction tasks
        @compact_base_path = File.join(@znode_base,"compact") # /admin/hbase/cluster/compact

        # base znode path for locks
        @lock_base_path = File.join(@compact_base_path,"lock",File.basename(@input).gsub(/\..*/,'' )) # /admin/hbase/cluster/compact/lock/compact_1234567891

        # lock znode name
        @lock_node=File.join(@lock_base_path,"lock") # /admin/hbase/cluster/compact/lock/compact_1234567891/lock

        # znode path for keeping state of this run
        @inprogress_path = File.join(@compact_base_path,"inprogress",File.basename(@input).gsub(/\..*/,'' ))

        # create /admin if it doesn't exists
        unless HbaseCtl.dryrun
          unless @admin.znode_exists?("/admin")
            @admin.create_znode("/admin",{ :mode => 0 }) 
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode /admin")
          end
	else
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode /admin")
        end
        
        # create /admin/hbase/cluster-hb1
        unless HbaseCtl.dryrun
          unless @admin.znode_exists?(@znode_base)
            @admin.create_znode(@znode_base,{ :mode => 0 , :nested => true })  # this doesn't raise any exception
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@znode_base")
          end
        else
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@znode_base")
        end

        # create /admin/hbase/cluster-hb1/compact
        unless HbaseCtl.dryrun
          unless @admin.znode_exists?(@compact_base_path)
            @admin.create_znode(@compact_base_path, { :mode => 0}) 
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@compact_base_path")
          end
        else
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@compact_base_path")
        end

        # create lock base /admin/hbase/cluster-hb1/compact/lock/compact_1234567891
        unless HbaseCtl.dryrun
          unless @admin.znode_exists?(@lock_base_path)
            @admin.create_znode(@lock_base_path, { :mode => 0, :nested => true }) 
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@lock_base_path")
          end
        else
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@lock_base_path")
        end

        unless HbaseCtl.dryrun
          unless @admin.znode_exists?(@inprogress_path)
            @admin.create_znode(@inprogress_path, { :mode => 0, :nested => true }) 
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@inprogress_path")
          end
        else
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created znode #@inprogress_path")
        end

       
        rescue Exception => e
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# #{e.class} #{e.message} #{e.backtrace}")
          raise e
      end


      # verify the lock node
      # @params
      #    node name in zookeepr
      # @returns 
      #   TrueClass or FalseClass
      def verify_lock(lock)
        return lock if @dry_run
        @admin.znode_exists?(lock)
      end

      # main function to do the job
      # @params none
      # @return none
      def run!
         
        return if not runnable?

        # acquire lock first. This waits till we get the lock
        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# acquiring lock")
        my_lock = acquire_lock
        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# acquired  lock #{my_lock}")

        # Fixed size array for work queue
        work_queue = SizedQueue.new(@concurrency)
        thread_count = @concurrency
        threads = Array.new(thread_count)

        # add a monitor so we can notify when a thread finishes
        threads.extend(MonitorMixin)

        # add a condition variable on the monitored array to signal consumer to check the thread array
        threads_available = threads.new_cond
        
        # variable to signal that more work is added to the queue
        work_done = false

        # coordinate access to shared resources
        mutex = Mutex.new

        # if i still have the lock, just in case if connection is lost
        if verify_lock(my_lock)
          # consumer thread that does the compaction
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Creating consumer thread")
          
          consumer_thread = Thread.new do 
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Created consumer thread #{Thread.current}")
            loop do
              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Consumer thread: #{Thread.current} work_done: #{work_done} work_queue: #{work_queue.length}")
              # stop looping when there are no more work
              break if work_done && work_queue.length == 0
              found_index = nil
              
              # modify shared resources in synchronize block
              threads.synchronize do

                threads_available.wait_while do 
                  threads.select { |thread|
                    thread.nil? || thread.status == false || thread["finished"].nil? == false}.length == 0
                end

                found_index = threads.rindex { |thread|
                  thread.nil? || thread.status == false || thread["finished"].nil? == false}
              end

              # get a new table from the work queue
              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Consumer thread: #{Thread.current} work_done: #{work_done} work_queue: #{work_queue.length}")
              table = work_queue.pop

              break if table.nil? || table.empty?
             
              #this creates a thread each time a thread finishes . revisit later to improve the logic
              threads[found_index] = Thread.new(table) do  
                # Compaction logic
                begin
                  current_thread = "Thread: #{Thread.current} :" 
                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# Started ")

		  # check whether I got a hash or  raise exception
                  case table
                  when Hash
                    table.each_pair do |table_name,regions| 
		      HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# consumer thread got work for #{table_name}")
                      compaction_queue = Array.new
                      num_regions_in_compaction = 0 
                      #if there are any znodes then there might be compaction running for that table. Script should not create a compaction storm.
                      
                      # create a node for table /admin/hbase/cluster/compact/compact_UNIXTIME/inprogress/table
                      table_znode_path = File.join(@inprogress_path,table_name)

		      if( @admin.table_exist?(table_name) )
                        if ( ! @admin.znode_exists?(table_znode_path) )
                          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{table_znode_path} znode doesn't exist creating it " )
			  # create a persistent node
                          table_znode=@admin.create_znode(table_znode_path, { :info => @input, :mode => 0 , :nested => true}) 
                          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# Created #{table_znode_path}") unless table_znode.empty? or table_znode.nil?
                        else
                          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{table_znode_path} exists not creating it" )

                          #since the table_znode exist verify the chidren/region nodes and verify if they are doing compaction
                          #This is needed to stay away from starting too many compactions on the table
                          begin
                            regions_in_znode = @admin.get_children(table_znode_path,false)

                            regions_in_znode.each do |region_name|
                              # get_compaction_state return an array of hashes
                              compaction_state = @admin.get_compaction_state(region_name)

                              # get the first value , and key of the hash
			      case compaction_state.first[region_name]
                              when HbaseCtl::Constants::COMPACTION_STATE_MAJOR, HbaseCtl::Constants::COMPACTION_STATE_MINOR, HbaseCtl::Constants::COMPACTION_STATE_MAJOR_AND_MINOR 
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} currently compacting, skipping this region")
			        num_regions_in_compaction += 1 # currently this is not used but might need in future to throttle regions across compaction instances

                                mutex.synchronize do 
                                  @progress << { "table" => table_name, "region" => region_name , "task" => @command , "status" => "skipped", "time" => Time.now.to_s} 
                                end

                              else
                                #HbaseCtl.log("#{current_thread}#@command:#{current_thread}:# table: #{table_name} region: #{region_name} not compacting, adding to the queue")
                                #compaction_queue << region_name
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} not compacting, removing the znode")
                                # if regin needs compaction next input will have this region. Revisit this to do the compaction anyway 
                                @admin.delete_znode(File.join(table_znode_path,region_name),-1)
                              end #case ends

                            end # block ends


                          rescue org.apache.zookeeper.KeeperException, java.lang.InterruptedException, java.io.IOException  => e
                            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{e.message} #{e.inspect} #{e.backtrace}")
                          end #begin ends

                        end #if of znode (table) exist, ends

                        
                        regions.each do |region_name|
                          begin
                            # get HRegionInfo,Servername pair
                            region_pair = @admin.get_region(region_name)
                           
                            if region_pair.nil?
                              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} unable to validate region")

                              mutex.synchronize do
                                @progress << { "table" => table_name, "region" => region_name , "task" => @command , "status" => "invalid", "time" => Time.now.to_s}
                              end

                              next
                            end

                            region_path = File.join(table_znode_path,region_name)
                            if @admin.znode_exists?(region_path) 
                              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{region_path} exists .. checking the compaction state ")
     			      compaction_state = @admin.get_compaction_state(region_name) 

                              case compaction_state.first[region_name]
                              when HbaseCtl::Constants::COMPACTION_STATE_MAJOR_AND_MINOR 
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} current compaction state: major/minor. Not initiating compaction")
                                mutex.synchronize do
                                  @progress << { "table" => table_name, "region" => region_name , "task" => @command , "status" => "skipped", "time" => Time.now.to_s}
                                end
                              when HbaseCtl::Constants::COMPACTION_STATE_MAJOR
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} current compaction state: major. Not initiating compaction ")                
                                mutex.synchronize do
                                  @progress << { "table" => table_name, "region" => region_name , "task" => @command , "status" => "skipped", "time" => Time.now.to_s}
                                end
                              when HbaseCtl::Constants::COMPACTION_STATE_MINOR
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} current compaction state: minor. Not initiating compaction")
                                mutex.synchronize do
                                  @progress << { "table" => table_name, "region" => region_name , "task" => @command , "status" => "skipped", "time" => Time.now.to_s}
                                end
                              else
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} compaction state: none. Initiating compaction")
                                compaction_queue << region_name
                              end #case ends
                              
                            else
                              # create the region znode 
			      region_znode = @admin.create_znode(region_path,{:mode => 0 , :info => "#{Time.now.to_i}"}) # put a meaningful information ?
                              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# Created #{region_path}") unless region_znode.empty? || region_znode.nil?
                              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name}  Initiating compaction")
                              compaction_queue << region_name
                            end #if ends

                          rescue java.io.IOException, java.lang.InterruptedException => e
			    HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{e.message} #{e.inspect} #{e.backtrace}")
                            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# skipping region #{region_name}")
                            mutex.synchronize do
                              @progress << { "table" => table_name, "region" => region_name , "task" => @command , "status" => "skipped", "time" => Time.now.to_s}
                            end
                          end # begin end
                        end #region iterator block ends

                  
		        # number of regions to compact in one pass.
                        num_region_threshold = (@work_queue_percent.fdiv(100) * compaction_queue.length).ceil
                        num_region_threshold = compaction_queue.length if num_region_threshold > compaction_queue.length
                        limit = num_region_threshold

                        if compaction_queue.length <= 10 # if the number of regions < 10 do compaction for all 
                          limit = compaction_queue.length 
                        end
                        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name}  region_concurrency: #{limit} compaction_queue_length: #{compaction_queue.length} ")
                  
                        compaction_work_queue = Array.new
                        done_queue = Array.new
                        inprogress_queue = Array.new
                        loop_count = 0
                  
                        # iterate over each region that we selected and do compaction
                        # pause if we reached the limit , till we have any free slot
                        compaction_queue.each do |region_name|
                          loop_count +=1
                          compaction_work_queue << region_name
                          inprogress_queue << region_name
                          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region_concurrency: #{limit} compaction_work_queue: #{compaction_work_queue.length} inprogress_queue: #{inprogress_queue.length}  compaction_queue_length: #{compaction_queue.length} ")
                          region_path = File.join(table_znode_path,region_name)
                          begin 
                            @admin.compact(region_name)
                          rescue java.io.IOException, java.lang.InterruptedException => e
                            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{e.message} #{e.inspect} #{e.backtrace}")
                            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region_name} failed compaction. ")
                            mutex.synchronize do 
                              @progress << { "table" => table_name, "region" => key, "task" => @command , "status" => "failed", "time" => Time.now.to_s}
                            end
                            # cleanup znode since there was an exception
                            @admin.delete_znode(region_path,-1) 
                            inprogress_queue.delete(region_name)
                            compaction_work_queue.delete(region_name)
                            loop_count -=1
                          end #begin end
 
                          if loop_count == limit
                            loop do
                            # sleep 30 seconds and check the status of compaction
                              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# sleeping for 30 seconds till compactions finish")
                              sleep 30
                        
                              # returns the status of each region as an array of hashes 
                              compaction_status = @admin.get_compaction_state(inprogress_queue)

                              compaction_status.each do |region_status| # region will be a hash with region name as key
                                region,status = region_status.shift
                                region_path = File.join(table_znode_path,region)

                                if status ===  HbaseCtl::Constants::COMPACTION_STATE_NONE # no compaction is running

                                  done_queue << region # add finished region to done_queue

                                  inprogress_queue.delete(region) || HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} not found in inprogress_queue")

                                  loop_count -= 1

                                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} compaction completed")
		                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} removing znode #{region_path}")

                                  # this region is done. Delete the znode
                                  begin
                                    @admin.delete_znode(region_path,-1)
		                    HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} removed znode #{region_path}")

                                  rescue org.apache.zookeeper.KeeperException, java.lang.InterruptedException, java.lang.IllegalArgumentException => e
                                    HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} caught exception while deleting znode #{region_path}")
			          end
                              
                                  # update the progress to the master thread
                                  mutex.synchronize do 
                                    @progress << { "table" => table_name, "region" => region, "task" => @command , "status" => "done", "time" => Time.now.to_s}
                                  end
                             

                                else
                                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} Compaction still in progress status: #{status}") 
                                  begin
                                    data,stat = @admin.get_znode(region_path)
                                    unless stat.nil?
                                      HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} updating znode #{region_path} with status : #{status}") 
                                      version = stat.getVersion
                                      @admin.set_znode(region_path,"#{status} #{Time.now}", version) 
                                    end
                                  rescue org.apache.zookeeper.KeeperException, java.lang.InterruptedException => e
                                     HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} region: #{region} caught exception while updating znode #{region_path}")
                                  end
                                end #if ends 
                              end # block ends

                              if done_queue.length == compaction_queue.length 
				# We are done with this table. Now delete the table znode and break out of the loop and finishes thread
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} Finished #{done_queue.length} out of #{compaction_queue.length}")
                                # delete this table znode
                                begin
                                  node_children = @admin.get_children(table_znode_path,false)
                                  if node_children.length > 0
                                    HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} #{table_znode_path} is not empty. Not deleting")
                                  else
                                    HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} deleting table znode #{table_znode_path}")
                                    @admin.delete_znode(table_znode_path,-1)
                                    HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} deleted table znode #{table_znode_path}")
                                  end

                                rescue org.apache.zookeeper.KeeperException, java.lang.InterruptedException, java.lang.IllegalArgumentException => e
                                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name}  caught exception while deleting table znode #{table_znode_path}") 
                                end

                                break

                              elsif compaction_work_queue.length < compaction_queue.length && loop_count < limit
                                break

                              elsif compaction_work_queue.length == compaction_queue.length && done_queue.length < compaction_queue.length
                                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table: #{table_name} waiting for all regions to finish compaction. Finished #{done_queue.length} out of #{compaction_queue.length}")
                              end #if ends

                            end # loop ends
                          end # if loop_count ends
		        end #compaction queue block ends
                      else # else for if table exists
                        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# table #{table_name} doesn't exist, skipping ")
                      end # if ends


                    end #table iterator block end

                  else #case else
                    raise ArgumentError.new("#{HbaseCtl.hostname}:#@command:#{current_thread} Hash Expected but received #{table.class}") 
                  end #case end
                
                # rescue block for thread
                rescue  ArgumentError, java.lang.InterruptedException, java.lang.IllegalArgumentException => e
                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:# #{e.message} #{e.inspect} #{e.backtrace}")
                  raise e
                end #begin end
                      
                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{current_thread}:#  Finished")


                # Mark this thread as done so the consumer knows there is a free spot
                Thread.current["finished"] = true

                # signal i am done 
                threads.synchronize do 
                  threads_available.signal
                end

              end # inner thread end
            end #loop end
          end #outer thread end


          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# Creating Producer thread")
          producer_thread = Thread.new do 
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{Thread.current}:# Producer thread")

            @seed[@cluster].each_pair do |table,region|
              # this might be blocked when the queue is full
              HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{Thread.current}: Producer thread adding table #{table}")
              work_queue << { table => region } 
            
              # tell the consumer to check the threads array so it can attempt to schedule the next job if free spot exists
              threads.synchronize do
                threads_available.signal
              end
            end

            work_done = true
          end

          # Thread for renewing the ticket if the script runs too long
          kerberos_renewer_thread = Thread.new do
            HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{Thread.current}:# Kerberos Renewer thread started")
            loop do
              begin
                unless HbaseCtl.renew_ticket
                  HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{Thread.current}:# Kerberos ticket renewal failed")
                end
                #sleep 30 mins between renewal
                sleep 1800
              rescue RuntimeError => e
                HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{Thread.current}:# #{e.message} #{e.inspect} #{e.backtrace}")
              end
            end
          end

          # join both the producer and consumer threads so the main thread doesn't exist while the child threads are doing the work
          #
          producer_thread.join
          consumer_thread.join

        # join on the child threads to allow them to finish
          threads.each do |thread|
            thread.join unless thread.nil?
          end

          #kill the renewer thread
          kerberos_renewer_thread.kill
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#{kerberos_renewer_thread.inspect}:# Kerberos Renewer thread stopped ") 

        end # if for verify_lock ends

        # remove  @inprogress_path /admin/hbase/cluster/hbasectl/compact/inprogress/compaction_1382576717/
        node_children = @admin.get_children(@inprogress_path,false)
        if node_children.length > 0
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# #@inprogress_path is not empty, not deleting")
        else
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# deleting inprogress znode  #@inprogress_path")
          @admin.delete_znode(@inprogress_path,-1)
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# deleted inprogress znode #@inprogress_path")
        end


        rescue Exception => e
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# #{e.class} in main thread #{e.message} #{e.backtrace}")
          raise e
        ensure
          unlock(my_lock)
          

          # dump the progress/status upon exit
          # group the collection based on table name
          data_to_dump = @progress.group_by do |h|
            h.fetch("table")
          end
          yaml_store = YAML::Store.new("#{@input}.done")
          yaml_store.transaction do
            yaml_store[@cluster] = data_to_dump
          end
        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# #{__method__} Finished")
      end

      def acquire_lock
        return "dryrun/lock" if @dry_run
	lock = Object.new
        lock.extend(MonitorMixin)
        cond = lock.new_cond
        zk_path = @admin.create_znode(@lock_node)
        lock.synchronize do
          while true do
            nodes = @admin.get_children(@lock_base_path,Watch.new(lock,cond))
            nodes.sort!
            if ( zk_path =~ /#{nodes.first}/ )
              return zk_path
            else
              cond.wait
            end
          end
        end
        rescue Exception => e
          message = "Unable to get the lock: "
	  message << "#{e.class} #{e.message} #{e.backtrace.inspect} "
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:#  #{message}")
          raise e
      end
     

      def unlock(lock)

        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# removing lock  #{lock} ") 
        @admin.delete_znode(lock,-1)
        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# removed lock  #{lock} ") 

        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# removing lock base znode #@lock_base_path ") 
        @admin.delete_znode(@lock_base_path,-1)
        HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# removed lock base znode #@lock_base_path ") 

        rescue Exception => e
          message = "Unable to remove the lock: "
          message << "#{e.class} #{e.message} #{e.backtrace.inspect} "
          HbaseCtl.log("#{HbaseCtl.hostname}:#@command:# #{__method__}   #{message}")
      end

        

      def runnable?
        mandatory = Array.new
        mandatory << "cluster"  if ( @cluster.nil? || @cluster.empty? )
        mandatory << "file" if ( @input.nil? || @input.empty? )

        if ( mandatory.length > 0 )
         return false
          #raise ArgumentMissing.new("mandatory arguments missing #{mandatory}")
        else
          return true
        end

      end



      private
      def load_input(input)

        return {} unless File.exist?(input)
        data=YAML.load_file(input)

        rescue SystemCallError => e
          HBaseCtl.log("Got exception #{e.class} #{e.message}")
      end

    end # class Compactor ends

  end # module Tools ends

end # module HbaseTools ends


