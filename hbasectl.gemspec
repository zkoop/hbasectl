Gem::Specification.new do |s|
  s.name 		= 'hbasectl'
  s.version		= '1.0.1'
  
  s.authors 		= [ "Anish Mathew" ]
  s.email		= %q[zkoop.o@gmail.com]

  s.summary		= %q[tool to schedule maanged splits and compactions on an hbase cluster]
  s.executables		= [ "hbasectl" ]
  s.files		= [
			"lib/hbasectl/admin.rb",
			"lib/hbasectl/tools/compactor.rb",
			"lib/hbasectl/tools/list.rb",
			"lib/hbasectl/tools/splitter.rb",
			"lib/hbasectl/tools.rb",
			"lib/hbasectl.rb",
			"bin/hbasectl"
		 ]

  s.homepage		= %q[https://github.com/zkoop/hbasectl]
  s.require_paths	= [ "lib" ]

  s.add_dependency('subcommand', ">= 1.0.6")

end
