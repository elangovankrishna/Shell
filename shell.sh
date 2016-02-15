#!/bin/bash

###################################
### AUTHOR: KRISHNA E           ###
### MODIFIED:         #############
###################################

#Change path to working directory
cd ~/git/reserves/asd_navigator/asd_nav_m360_merge/se

if [ $? -ne 0 ]; then
	echo "Invalid path for working directory"
	exit 1
else
	echo "Switched to provided working directory"
fi

#Get the hive14 path
export PATH=$PATH:/usr/local/bin
 
#Point to the configuration file
source ./se_reserves_config.dat

if [ $? -ne 0 ]; then
        echo " Failure in sourcing the config file "
       exit 1
fi

#Colors
red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

echo "{hiveconf:RES}"$RES
echo "{hiveconf:ASD_SE}"$ASD_SE
echo "{hiveconf:ROS_SE}"$ROS_SE
echo "{hiveconf:CCR_SE}"$CCR_SE

#Get the working dates to be passed for the HQL's 

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
day=`echo $date | awk -F\- '{print $3}'`

#Run Stagea of service request 
start_time="${red}Start time: "$(date +%Y-%m-%d_%T)${reset}
echo "${red} Stagea Start time: "$start_time ${reset}

hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf YEAR=$yr --hiveconf MONTH=$mth --hiveconf DAY=$day --hiveconf RES=$RES --hiveconf ASD_SE=$ASD_SE --hiveconf ROS_M=$ROS_SE --hiveconf CCR_SE=$CCR_SE -f se_stagea_reserves.hql

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error on stagea run of SR"  elangovank@aetna.com
        exit 1
else 	echo "SUCCESS SR *** 'echo $date' *** 'date'" | mailx -s "SR Stagea completed successfully" elangovank@aetna.com
		
fi

echo "${green} Stagea End time: "$(date +%Y-%m-%d_%T)${reset}

# Run stageb of service request

#Table creation HQL's
echo "${green} Stageb Start time: "$start_time ${reset}
hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf RES=$RES -f se_stageb_tbl_reserves.hql

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error creating tables in stageb of SR "  elangovank@aetna.com
        exit 1
fi

echo "${green} Table creation HQL End time: "$(date +%Y-%m-%d_%T)${reset}

#Initial all the table for the run date
echo "${green} Initilize HQL Start time: "$(date +%Y-%m-%d_%T)${reset}
hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf YEAR=$yr --hiveconf MONTH=$mth --hiveconf DAY=$day --hiveconf RES=$RES --hiveconf ASD_SE=$ASD_SE --hiveconf ROS_SE=$ROS_SE --hiveconf CCR_SE=$CCR_SE -f se_stageb_intz.hql

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error in Initilize of Stageb table in SR "  elangovank@aetna.com
        exit 1
fi
echo "${green} Initilize HQL End time: "$(date +%Y-%m-%d_%T)${reset}

#Run the pig script which does the main join logic
echo "${green} Pig Script Start time: "$(date +%Y-%m-%d_%T)${reset}

pig -useHCatalog -param RES=$RES -param ASD_SE=$ASD_SE -f se_stageb_pig.pig

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error in pig script of stageb SR "  elangovank@aetna.com
        exit 1
fi

echo "${green}Pig script End time: "$(date +%Y-%m-%d_%T)${reset}

#Run the final script to load the stageb table

echo "${green} Final HQL Start time: "$(date +%Y-%m-%d_%T)${reset}
hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf RES=$RES -f se_stageb_fin.hql

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error updating final Stageb table for SR "  elangovank@aetna.com
        exit 1
else
		echo "SUCCESS SR *** 'echo $date' *** 'date'" | mailx -s "SE Stageb completed successfully in SR " elangovank@aetna.com
fi

echo "${green}Stageb End time: "$(date +%Y-%m-%d_%T)${reset}



#Run stagen for Service Request 

echo "${green} Stagen Start time: "$start_time ${reset}
hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf RES=$RES --hiveconf ASD_SE=$ASD_SE -f se_stagen_reserves.hql

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error on Stagen of SR "  elangovank@aetna.com
        exit 1
else
		echo "SUCCESS SR *** 'echo $date' *** 'date'" | mailx -s "SE Stagen completed successfully in SR " elangovank@aetna.com
fi

echo "${green}End time: "$(date +%Y-%m-%d_%T)${reset}


#Run stager of Service Request

echo "${green}Start time: "$start_time ${reset}

hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf YEAR=$yr --hiveconf MONTH=$mth --hiveconf DAY=$day --hiveconf RES=$RES -f se_stager_reserves.hql

if [ $? -ne 0 ]; then
        echo "ERROR SE *** `echo $date` *** `date`" | mailx -s "Error on Stager of SR "  elangovank@aetna.com
        exit 1
fi
echo "${green}End time: "$(date +%Y-%m-%d_%T)${reset}

## run Stage SR
#/opt/quest/bin/vastool -k ~/DoNotTouch/s061332.keytab kinit

start_time="${red}Start time: "$(date +%Y-%m-%d_%T)${reset}

hive14 --hiveconf tez.queue.name=prodrevenue --hiveconf tez.session.am.dag.submit.timeout.secs=1000 --hiveconf tez.session.client.timeout.secs=1500 --hiveconf DATE=$date --hiveconf RES=$RES -f se_sr_reserves.hql

if [ $? -ne 0 ]; then
        echo "ERROR SR *** `echo $date` *** `date`" | mailx -s "Error on Stager of SR "  elangovank@aetna.com
        exit 1
else
		echo "SUCCESS SR *** 'echo $date' *** 'date'" | mailx -s "SE Stager completed successfully in SR " elangovank@aetna.com
fi
echo "${green}End time: "$(date +%Y-%m-%d_%T)${reset}

echo "${red}===================="${reset}
echo "${green}Script:"$start_time ${reset}
echo "${green}End time: "$(date +%Y-%m-%d_%T)${reset}
echo "${green}Success CEI: `date`"${reset}
