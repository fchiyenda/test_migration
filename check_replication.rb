#!/usr/bin/ruby -w

require 'rest-client'
require 'json'

def replication_status(u,p,src,dst)
	check_replication_status = RestClient.get("#{u}:#{p}@localhost:5984/_active_tasks")
  status = JSON.parse(check_replication_status)
  if status.size == 0 then
  	#start replication
  	begin
    	RestClient.post("http://#{u}:#{p}@localhost:5984/_replicate","{\"source\":\"#{src}\",\"target\":\"#{dst}\", \"continuous\":true}",content_type: :json)
		rescue => e
			puts e.response
		end
	
   	$num += 1
  end
end

u = ARGV[0]
p = ARGV[1]
src = ARGV[2]
dst = ARGV[3]

i = 0
a = ['|','/','-','\\']
n = 0
$num = 0
while i != -1
	sleep (1)
	replication_status(u,p,src,dst)
	n = 0 if n == 4 # reset n to zero if it has printed the last character

	printf("\r Restarted Replication: %.d times Running status: %s", $num, a[n])
	n += 1
end