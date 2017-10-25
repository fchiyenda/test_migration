#!/usr/bin/ruby -w

require 'rest-client'
require 'json'

def replication_status(u,p,src,dst)
	check_replication_status = RestClient.get("#{u}:#{p}@localhost:5984/_active_tasks")
  status = JSON.parse(check_replication_status)
  if status.size == 0 then
  	#start replication
  begin
  	RestClient.post('http://localhost:5984/_replicate','{"source":"#{src}","target":"#{dst}", "continuous":true}',content_type: :json)
  rescue => e
  	raise e.inspect
  end
  	@num += 1
  end
end

u = ARGV[0]
p = ARGV[1]
src = ARGV[2]
dst = ARGV[3]

i = 0
@num = 0
while i != -1
	sleep 60
	replication_status(u,p,src,dst)
	printf("\r Restarted Replication: %.d times", @num)
end