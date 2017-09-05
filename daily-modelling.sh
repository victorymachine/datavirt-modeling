#!/bin/bash

source venv/bin/activate
python jdv_modelling_ora.py -i table_list.csv
python jdv_modelling_hive.py -i table_list.csv
python jdv_modelling_xml.py

if /usr/share/jbossas/bin/jboss-cli.sh -c --controller=localhost:9999 --commands="undeploy trace-dev-vdb.xml, quit"
then
	echo trace-dev-vdb.xml UNDEPLOYED successfully	
	if /usr/share/jbossas/bin/jboss-cli.sh -c --controller=localhost:9999 --commands="deploy /tmp/trace-dev-vdb.xml, quit"
		echo trace-dev-vdb.xml DEPLOYED successfully
else 
	echo UNDEPLOY failed
fi