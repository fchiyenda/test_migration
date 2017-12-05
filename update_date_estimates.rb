#!/usr/bin/ruby -w

require 'rubygems'
require 'rest-client'
require 'json'
require 'colorize'

def update_person_dob(person)
	#Check if day and month are both estimated
	check_date = person['birthdate'].split('/')
	puts check_date.inspect

	if check_date[0].include?('?') && check_date[1].include?('?') then
		check_date[0] = '01'
		check_date[1] = '07'
	elsif check_date[0].include?('?') then
		check_date[0] = '15'
	end

	dob = "#{check_date[2]}\-#{check_date[1]}\-#{check_date[0]}"

  puts dob.inspect
	raise dob.inspect if check_date[0].include?('?') && check_date[1].include?('?')

	url = "http://#{$h}:5984/#{$couchdb}/#{person['_id']}"
	person['birthdate'] = dob
	person['birthdate_estimated'] = 1


	params = person.to_json
 
	#response = RestClient.put url,params,:content_type => 'application/json'
	$log.syswrite("#{person['_id']} \n")
  
  #puts response.inspect 
	#raise person['_id']
end

def update_date_estimates(h,u,p,couchdb)
#Get first batch of ids from couchdb
	i = 0
	n = 100000
	$log = File.new("log/dde_update_bithdate_estimated.log", "w")
 #Get total number of documents
 puts "Getting Total number of records"
  count = RestClient.get("http://#{h}:5984/#{couchdb}/_all_docs?skip=0&limit=2")
  cnt = JSON.parse(count)

  while i < cnt['total_rows'] do
  	puts "processing #{i} to #{n}"
		docs = RestClient.get("http://#{h}:5984/#{couchdb}/_all_docs?include_docs=true&skip=#{i}&limit=#{n}")
		
		data = JSON.parse(docs)

	  data['rows'].each do |person|
			puts person['doc']['birthdate']
			if !person['doc']['birthdate'].nil? then
				if person['doc']['birthdate'].include? '??' then
					puts person['doc'].inspect
					update_person_dob(person['doc'])
				end
			end
		end
		i += 100000
		n += 100000 
	end
end

#Start program

#Get parameters from terminal
$h = ARGV[0]
$u = ARGV[1]
$p = ARGV[2]
$couchdb = ARGV[3]

if $h.nil? || $u.nil? || $p.nil? || $couchdb.nil? then
  puts 'Please execute command as "ruby update_date_estimates.rb host_ip_address couchdb_username couchdb_password couchdb_name" '.colorize(:red)
  exit
end
update_date_estimates($h,$u,$p,$couchdb)