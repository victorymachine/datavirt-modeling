import csv
import argparse
import jaydebeapi
import jpype
import json

def read_col_oracle(con_str,table_name):
	
	jdbc_driver_name = 'oracle.jdbc.OracleDriver'
	jdbc_driver_loc = './jdbc/ojdbc6.jar'
	url = 'jdbc:oracle:thin:@10.54.9.105:4713/TRACEDEV'

	args='-Djava.class.path=%s' % jdbc_driver_loc
	jvm = jpype.getDefaultJVMPath()
	if not jpype.isJVMStarted(): 
		jpype.startJVM(jvm)

	conn_ora = jaydebeapi.connect(jdbc_driver_name,[url,'HOBSIRASD','H0b51ras'],jdbc_driver_loc)
	curs_ora = conn_ora.cursor()
	print table_name
	query_column_info = "select COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE FROM USER_TAB_COLUMNS where table_name =  '%s' ORDER BY COLUMN_ID" % (table_name.rstrip())

	curs_ora.execute(query_column_info)
	column_info = []
	val = curs_ora.fetchall()
	column_info.append(table_name.rstrip())
		
	for column_name_info in val:
		column_info.append(column_name_info)	
		


        	
	return column_info

def write_to_csv(datamart_source):
	with open('./datamart.csv', 'w') as csvfile:
	    fieldnames = ['TABLE_NAME','COLUMN_NAME', 'DATA_TYPE', 'DATA_LENGTH', 'NULLABLE']
	    
	    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	    writer.writeheader()
	    #writer.writerow({'COLUMN_NAME' : , 'DATA_TYPE' : , 'DATA_LENGTH' : , 'NULLABLE'})
	    for table in datamart_source:
	    	for column_info in table[1:]:
	    		writer.writerow({'TABLE_NAME':table[0],'COLUMN_NAME' : column_info[0] , 'DATA_TYPE' : column_info[1], 'DATA_LENGTH' : int(column_info[2]), 'NULLABLE' :column_info[3]})

	with open('./datamart.json', 'w') as jsonfile:
		jsonfile.write(json.dumps(datamart_source, sort_keys=True,indent=4, separators=(',', ': ')))
def main():

	parser = argparse.ArgumentParser()
	parser.add_argument('-i','--infile', dest='infile', required=True)

	args = parser.parse_args()

	datamart_source = []
	hive_source = []
	datamart_foreign = []
	
	with open(args.infile, 'rb') as csvfile:
		spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for row in spamreader:
			if row[0] == 'datamart':
				datamart_source.append(read_col_oracle(row[1],row[2]))

	write_to_csv(datamart_source)

main()