import csv
import argparse
import jaydebeapi
import jpype
import json

def read_col_hive(con_str,table_name):
	
	jdbc_driver_name = 'org.apache.hive.jdbc.HiveDriver'
	jdbc_driver_loc = './jdbc/hive-jdbc-standalone_mod-cd.jar'

	url = 'jdbc:hive2://10.14.91.87:10000/default'
	args='-Djava.class.path=%s' % jdbc_driver_loc
	jvm = jpype.getDefaultJVMPath()
	if not jpype.isJVMStarted(): 
		jpype.startJVM(jvm)
	
	conn_hive = jaydebeapi.connect(jdbc_driver_name,[url,'trace',''],'./jdbc/hive-jdbc-standalone_mod-cd.jar')
	curs_hive = conn_hive.cursor()

	query_column_info = "desc %s" % (table_name.rstrip())
	print query_column_info
	curs_hive.execute(query_column_info)
	column_info = []
	val = curs_hive.fetchall()
	column_info.append(table_name.rstrip())
	for column_name_info in val:
		column_info.append(column_name_info)
	return column_info

def write_to_csv(hive_source):
	with open('./hive.csv', 'w') as csvfile:
	    fieldnames = ['TABLE_NAME','COLUMN_NAME', 'DATA_TYPE']
	    
	    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	    writer.writeheader()
	    #writer.writerow({'COLUMN_NAME' : , 'DATA_TYPE' : , 'DATA_LENGTH' : , 'NULLABLE'})
	    for table in hive_source:
	    	for column_info in table[1:]:
		    	writer.writerow({'TABLE_NAME':table[0],'COLUMN_NAME' : column_info[0] , 'DATA_TYPE' : column_info[1]})
		    	#print column_info

	with open('./hive.json', 'w') as jsonfile:
		jsonfile.write(json.dumps(hive_source, sort_keys=True,indent=4, separators=(',', ': ')))
    #print "WRITTEN %s" % fname
	#print jsonstr
def main():

	parser = argparse.ArgumentParser()
	parser.add_argument('-i','--infile', dest='infile', required=True)

	args = parser.parse_args()

	hive_source = []

	with open(args.infile, 'rb') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for row in spamreader:
			if row[0] == 'hive':
				hive_source.append(read_col_hive(row[1],row[2]))
	
	write_to_csv(hive_source)

main()