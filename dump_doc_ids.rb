require 'rest-client'
require 'json'

def get_all_couchdb_ids(h1,cdb1,h2,cdb2)
	i = 0
	n = 0
	npid_array1 = []
	npid_array2 = []

#Get NPIDS for the first database
  
  puts "Processing first database \n"
	puts "Getting Total number of records from #{cdb1}"
  count = RestClient.get("http://#{h1}:5984/#{cdb1}/_all_docs?skip=0&limit=1")
  cnt = JSON.parse(count)

	while i <= 2 do 
		puts "Querying documents from #{h1} #{cdb1} from document #{i} to #{i + 200000}"
		ids = RestClient.get("http://#{h1}:5984/#{cdb1}/_all_docs?skip=#{i}&limit=2")

		puts "Parsing npid data"
		npids = JSON.parse(ids)
		
		puts "addding NPIDs to array"
		npids["rows"].each do |npid|
			npid_array1 << npid['id']
		end
		i += 200_000
	end
	puts npid_array1.count.inspect

#Get NPIDS for the second database

  puts "Processing second database \n"
	puts "Getting Total number of records from #{cdb2}"
  count = RestClient.get("http://#{h2}:5984/#{cdb2}/_all_docs?skip=0&limit=1")
  cnt = JSON.parse(count)

  puts cnt['total_rows'].inspect

	while n <= 2 do 
		puts "Querying documents from #{h2} #{cdb2} from document #{n} to #{n + 200000}"
		ids = RestClient.get("http://#{h2}:5984/#{cdb2}/_all_docs?skip=#{n}&limit=2")

		puts "Parsing npid data"
		npids = JSON.parse(ids)
		
		puts "addding NPIDs to array"
		npids["rows"].each do |npid|
			npid_array2 << npid['id']
		end
		n += 200_000
	end
	puts "#{cdb1}: #{npid_array1}"
	puts "#{cdb2}: #{npid_array2}"
	log = File.new("log/dump_doc_ids.log", "w")
	log.syswrite("#{cdb1} - #{cdb2}: \n \n #{npid_array1 - npid_array2} \n \n #{cdb2} #{cdb1}: \n \n #{npid_array2 - npid_array1}")
	puts "(Documents in #{h1} #{cdb1} and not in #{h2} #{cdb2}: #{(npid_array1 - npid_array2).count}"
	puts "(Documents in #{h2} #{cdb2} and not in #{h1} #{cdb1}: #{(npid_array2 - npid_array1).count}"
end

h1 = ARGV[0]
cdb1 = ARGV[1]
h2 = ARGV[2]
cdb2 = ARGV[3]

get_all_couchdb_ids(h1,cdb1,h2,cdb2)
