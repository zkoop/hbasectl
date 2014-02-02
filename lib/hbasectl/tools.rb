
module HbaseCtl
  module Tools
    class Base # abstract class 

      def initialize(opts = {} )
      end


      def self.silence_stream(*streams)
        on_hold = streams.collect {|stream| stream.dup }
        streams.each do |stream|
          stream.reopen(RUBY_PLATFORM =~ /mswin/ ? 'NUL:' : '/dev/null' )
          stream.sync=true
        end
        yield
      ensure
        streams.each_with_index do |stream,i|
          stream.reopen(on_hold[i])
        end
      end


    end # class Base end
  end # module Tools end
end # module HbaseTools end
