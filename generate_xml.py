import csv
import argparse
import json
import time

def write_foreign_table_dm(table_info):
	
	table_str = 'CREATE FOREIGN TABLE %s (\n' % (str(table_info[0]))
	for column_info in table_info[1:]:
		if column_info[3] == 'Y':
			null_option = ''
		else:
			null_option = 'NOT NULL '
		if column_info[1] == "VARCHAR2":
			table_str += '''	"%s" string(%s) %sOPTIONS(NAMEINSOURCE '"%s"', NATIVE_TYPE 'VARCHAR2'), \n'''%(column_info[0],int(column_info[2]),null_option,column_info[0])
		if column_info[1] == "NUMBER":
			table_str += '''	"%s" bigdecimal %sOPTIONS(NAMEINSOURCE '"%s"', NATIVE_TYPE 'NUMBER', FIXED_LENGTH 'TRUE'),\n '''%(column_info[0],null_option,column_info[0])
		else:
			print ''
	#str2 = ' %s, ' % (column[0])
	table_str = table_str[:-3]

	table_str += '''\n) OPTIONS(NAMEINSOURCE '"HOBSIRASD"."%s"', UPDATABLE 'TRUE')\n\n''' % (str(table_info[0]))
	return table_str

def write_foreign_table_hive(table_info):
	
	table_str = 'CREATE FOREIGN TABLE %s (\n' % (str(table_info[0]))
	for column_info in table_info[1:]:
		table_str += '''	"%s" string(2147483647) OPTIONS(NAMEINSOURCE '%s', NATIVE_TYPE 'STRING'), \n'''%(column_info[0], column_info[0])
	#str2 = ' %s, ' % (column[0])	
	table_str = table_str[:-3]
	table_str += '''\n) OPTIONS(NAMEINSOURCE '"hobs_preprocess"."%s"', UPDATABLE 'TRUE')\n''' % (str(table_info[0]))
	return table_str

def str_to_xml_dm(datamart_foreign):
	datamart_str=''
	for foreign in datamart_foreign:
		datamart_str+=foreign
	return datamart_str

def str_to_xml_hive(hive_foreign):
	hive_str = ''
	for foreign in hive_foreign:
		hive_str+=foreign
	
	return hive_str

def main():

	parser = argparse.ArgumentParser()
	#parser.add_argument('-i','--infile', dest='infile', required=True)

	args = parser.parse_args()

	datamart_source = []
	hive_source = []
	datamart_foreign = []
	hive_foreign = []
	

	#with open('./datamart.csv') as dmcsvfile:
	#	spamreader = csv.reader(dmcsvfile, delimiter=',', quotechar='"')
	#	for table in spamreader:
			#datamart_foreign.append(write_foreign_table(table))
	#		table_name = table
	#		for column in table:
	#			print column
	#print hive_source
	#for table in datamart_source:
	#	datamart_foreign.append(write_foreign_table(table))
	with open('./datamart.json') as dmjsonfile:
		dm_source=json.load(dmjsonfile)

	for table in dm_source:
		datamart_foreign.append(write_foreign_table_dm(table))
	
	with open('./hive.json') as dmhivefile:
		hive_source=json.load(dmhivefile)
	for table in hive_source:
		hive_foreign.append(write_foreign_table_hive(table))

	xml_str = '''<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<vdb name="trace-dev" version="1">
<description/>
<property name="validationDateTime" value="Wed Jul 12 12:39:29 MYT 2017"/>
<property name="validationVersion" value="8.12.5"/>'''
	
	hive_str_head = '''
<model name="hive_hobs_preprocess">
<source connection-jndi-name="java:/hivejdbc-hobs_preprocess" name="hive_hobs_preprocess" translator-name="hive"/>
<metadata type="DDL"><![CDATA[\n'''
	hive_str_tail = ''']]></metadata>
</model>'''

	datamart_str_head = '''
<model name="datamart_HOBSIRASD">
<source connection-jndi-name="java:/datamartjdbc-HOBSIRASD" name="datamart_HOBSIRASD" translator-name="jdbc-simple"/>
<metadata type="DDL"><![CDATA[\n'''
	datamart_str_tail = ''']]></metadata>
</model>'''
	if hive_foreign:
		xml_str+=hive_str_head
		xml_str+=str_to_xml_dm(hive_foreign)
		xml_str+=hive_str_tail
	if datamart_foreign:
		xml_str+=datamart_str_head
		xml_str+=str_to_xml_dm(datamart_foreign)
		xml_str+=datamart_str_tail

	xml_str+='\n</vdb>'
	file = open("trace-dev-vdb.xml","w") 
	file.write(xml_str)
	file.close()
	#table_info.append(read_col_oracle(con_str,'R_016_BILL_LATE_BRM'))	
	#table_info.append(read_col_oracle(con_str,'R_007_SUM_N_BRM_POID_IS_NULL'))
	#for table in table_info:
	#	tab_str = write_foreign_table(table)

main()