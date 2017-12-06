#!/usr/bin/ruby -w

require 'rubygems'
require 'rest-client'
require 'json'
require 'colorize'

def update_person_dob(person)
	#Check if day and month are both estimated
	if person['birthdate'].include?('-') then
		check_date = person['birthdate'].split('-')
	elsif person['birthdate'].include?('/')
		check_date = person['birthdate'].split('/')
	end 

	if check_date[0].include?('?') && check_date[1].include?('?') then
		check_date[0] = '01'
		check_date[1] = '07'
		$with_out_month += 1
		$log_with_out_month.syswrite("#{person['_id']} \n")
	elsif check_date[0].include?('?') then
		check_date[0] = '15'
    $with_month += 1
    $log_with_month.syswrite("#{person['_id']} \n")
	end

	dob = "#{check_date[2]}\-#{check_date[1]}\-#{check_date[0]}"
	url = "http://#{$h}:5984/#{$couchdb}/#{person['_id']}"
	person['birthdate'] = dob
	person['birthdate_estimated'] = 1

	params = person.to_json
 	response = RestClient.put url,params,:content_type => 'application/json'
end

def update_date_estimates(h,u,p,couchdb)
#Get first batch of ids from couchdb
  $with_out_month = 0
  $with_month = 0
	i = 0
	n = 100000
	lmt = 100000
	$log_with_month = File.new("log/dde_update_bithdate_estimated_with_month.log", "w")
	$log_with_out_month = File.new("log/dde_update_bithdate_estimated_with_out_month.log", "w")
 #Get total number of documents
 puts "Getting Total number of records"
  count = RestClient.get("http://#{h}:5984/#{couchdb}/_all_docs?skip=0&limit=2")
  cnt = JSON.parse(count)

  while i <= cnt['total_rows'] do
  	puts "processing #{i} to #{n}"
		docs = RestClient.get("http://#{h}:5984/#{couchdb}/_all_docs?include_docs=true&skip=#{i}&limit=#{lmt}")
		
		data = JSON.parse(docs)

	  data['rows'].each do |person|
			if !person['doc']['birthdate'].nil? then
				if person['doc']['birthdate'].include? '??' then
					update_person_dob(person['doc'])
				end
			end
		end
		i += 100000
		n += 100000
	end
  puts "NPIDS with Month: #{$with_month} NPIDS with out Month: #{$with_out_month}"
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