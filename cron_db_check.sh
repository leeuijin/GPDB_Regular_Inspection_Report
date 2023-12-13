#!/bin/bash
#--------------------------------------------------------------------------------
# Version |    Date    |                Conetents
#--------------------------------------------------------------------------------
#  v1.0   | 2019-12-10 | cron_db_check.sh created
#  v1.1   | 2019-12-17 | os version, log path modified
#  v1.2   | 2019-12-23 | gpdb 6.x supported
#  v1.3   | 2020-01-05 | pglog 22P06 code modified
#  v1.4   | 2020-03-17 | add postgres db
#  v1.5   | 2020-04-08 | table age query modified
#  v1.6   | 2020-05-15 | add max oid confirm
#  v1.7   | 2020-06-05 | age database modified, pglog 57P01 code modified
#  v1.8   | 2020-10-20 | add gp_bloat_diag, add pglog panic level
#                        add gpcc status, add dca version
#  v1.9   | 2020-10-29 | pglog query modified
#  v2.0   | 2021-01-05 | segment age query modified
#--------------------------------------------------------------------------------
# Crontab Expression(Monday AM 6H)
# 0 6 * * 1 sh /home/gpadmin/utilities/cron_db_check.sh > /dev/null 2>&1 &
#--------------------------------------------------------------------------------
source ~/.bash_profile

EXEC=$0 
EXEC_NAME=$(basename -- "$EXEC")
NOW=$(date +"%Y%m%d_%H%M%S")
NOW_YMD=$(date +"%Y%m%d")
MONTH=$(date +"%Y%m")
MONTH_AGO=$(date -d "-1 month" +"%Y%m")
LOG_DIR=/home/gpadmin/utilities/log
LOG_FILE="${LOG_DIR}/db_check_result_${NOW_YMD}_`hostname`.log"
STAT_LOG_DIR=/data/gpkrutil/statlog
DCA_VERSION_FILE=/etc/gpdb-appliance-version

echo "--------------------------------------------------" >> ${LOG_FILE}
echo "GPDB Check (v2.0)"                                  >> ${LOG_FILE}
echo "--------------------------------------------------" >> ${LOG_FILE}
echo "$EXEC : START DATE : `date "+%Y-%m-%d %H:%M:%S"`" | tee -a ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- whoami' | tee -a ${LOG_FILE}
whoami >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- kernel' | tee -a ${LOG_FILE}
uname -a >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- os version' | tee -a ${LOG_FILE}
cat /etc/system-release >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- gpstate' | tee -a ${LOG_FILE}
gpstate >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- gpstate -e' | tee -a ${LOG_FILE}
gpstate -e >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- gpdb env' | tee -a ${LOG_FILE}
env | grep ^GP >> ${LOG_FILE}
env | grep ^PG >> ${LOG_FILE}
echo "MASTER_DATA_DIRECTORY=$MASTER_DATA_DIRECTORY" >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo '-- dca version' | tee -a ${LOG_FILE}
if [ -e "$DCA_VERSION_FILE" ]; then
  echo -n "dca version : " >> ${LOG_FILE}
  cat $DCA_VERSION_FILE >> ${LOG_FILE}
else
  echo "not dca production" >> ${LOG_FILE}
fi

echo >> ${LOG_FILE}
echo "-- gpcc version/status" | tee -a ${LOG_FILE}
if [ "$GPCC_HOME" != "" ]; then
    echo "-- gpcc version" >> ${LOG_FILE}
    $GPCC_HOME/bin/gpcc -version >> ${LOG_FILE}

    echo >> ${LOG_FILE}
    echo "-- gpcc status" >> ${LOG_FILE}
    $GPCC_HOME/bin/gpcc status >> ${LOG_FILE}
else
    echo"-- gpcmdr version" >> ${LOG_FILE}
    gpcmdr --version >> ${LOG_FILE}
    echo "-- gpcmdr status" >> ${LOG_FILE}
    gpcmdr --status >> ${LOG_FILE}
fi

echo >> ${LOG_FILE}
echo "-- gpconfig - age" | tee -a ${LOG_FILE}
gpconfig -s xid_warn_limit >> ${LOG_FILE}
echo >> ${LOG_FILE}
gpconfig -s xid_stop_limit >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo "-- pg_class max oid" | tee -a ${LOG_FILE}
psql gpperfmon -c " select to_char(max(oid)::bigint, '999,999,999,999') as pg_class_max_oid from pg_catalog.pg_class " >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo "-- system message" | tee -a ${LOG_FILE}

EMCCONNECT_HISTORY_EXISTS_YN=`psql gpperfmon -At -c "
select case when count(*) > 0 then 'Y' else 'N' end exists_yn
  from information_schema.tables
 where table_schema = 'public'
   and table_name = 'emcconnect_history'
"`

echo "EMCCONNECT_HISTORY_EXISTS_YN=$EMCCONNECT_HISTORY_EXISTS_YN" >> ${LOG_FILE}

if [ "$EMCCONNECT_HISTORY_EXISTS_YN" = "Y" ]; then
    echo >> ${LOG_FILE}
    psql gpperfmon -c "select * from public.emcconnect_history where ctime > current_date -7 and severity <> 'Info' order by 1 desc limit 5000" >> ${LOG_FILE}
fi

GPDB_VERSION=`psql gpperfmon -t -c "select setting from pg_catalog.pg_settings where name = 'gp_server_version_num'" | tr -d ' '`
echo >> ${LOG_FILE}
echo "-- gpdb server version" | tee -a ${LOG_FILE}
echo "GPDB_VERSION=$GPDB_VERSION" >> ${LOG_FILE}

GPCC_PG_LOG_HISTORY_EXISTS_YN=`psql gpperfmon -At -c "
select case when count(*) > 0 then 'Y' else 'N' end exists_yn
  from information_schema.tables
 where table_schema = 'gpmetrics'
   and table_name = 'gpcc_pg_log_history'
"`

echo >> ${LOG_FILE}
echo "-- gpcc pg_log_history confirm" | tee -a ${LOG_FILE}
echo "GPCC_PG_LOG_HISTORY_EXISTS_YN=$GPCC_PG_LOG_HISTORY_EXISTS_YN" >> ${LOG_FILE}

if [ "$GPCC_PG_LOG_HISTORY_EXISTS_YN" = "Y" ]; then
    # GPDB 6.x
    echo >> ${LOG_FILE}
    echo "-- gpcc pglog panic history" | tee -a ${LOG_FILE}
    psql gpperfmon -c " select logtime
                             , loguser
                             , logdatabase
                             , logpid
                             , loghost
                             , logseverity
                             , logstate
                             , substring(logmessage, 1, 1000) as logmessage
                             , substring(logdetail, 1, 1000) as logdetail
                             , substring(loghint, 1, 1000) as loghint
                             , substring(logquery, 1, 1000) as logquery
                             , logquerypos
                             , substring(logcontext, 1, 1000) as logcontext
                             , substring(logdebug, 1, 1000) as logdebug
                             , logcursorpos
                             , logfile
                             , logline
                             , substring(logstack, 1, 500) as logstack
                          from gpmetrics.gpcc_pg_log_history
                         where logtime > current_date - 7
                           and logseverity = 'PANIC'
                         order by logtime desc
                         limit 100 " >> ${LOG_FILE}

    echo >> ${LOG_FILE}
    echo "-- gpcc pglog history" | tee -a ${LOG_FILE}
    psql gpperfmon -c " select logtime
                             , loguser
                             , logdatabase
                             , logpid
                             , loghost
                             , logseverity
                             , logstate 
                             , substring(logmessage, 1, 1000) as logmessage
                             , substring(logdetail, 1, 1000) as logdetail 
                             , substring(loghint, 1, 1000) as loghint
                             , substring(logquery, 1, 1000) as logquery
                             , logquerypos 
                             , substring(logcontext, 1, 1000) as logcontext
                             , substring(logdebug, 1, 1000) as logdebug
                             , logcursorpos 
                             , logfile
                             , logline 
                             , substring(logstack, 1, 500) as logstack
                          from gpmetrics.gpcc_pg_log_history
                         where logtime > current_date - 7
                           and logseverity not in ('INFO', 'PANIC')
                           and logstate not like '42%'
                           and logstate not in ('57014', '57P01', '08006', '3D000', '3F000', '22P06')
                         order by logtime desc
                         limit 5000 " >> ${LOG_FILE}

    echo >> ${LOG_FILE}
    echo "-- gpcc node size" | tee -a ${LOG_FILE}
    psql gpperfmon -c " select ctime, hostname, filesystem, pg_size_pretty(total_bytes) as total, pg_size_pretty(bytes_used) as used, pg_size_pretty(bytes_available) as available
                             , round(bytes_used::numeric / total_bytes::numeric * 100, 2) as used_percent
                          from gpmetrics.gpcc_disk_history
                         where ctime = (select max(ctime) from gpmetrics.gpcc_disk_history)
                           and filesystem not like '/boot%'
                         order by case
                                       when hostname = (select hostname from pg_catalog.gp_segment_configuration where content = -1 and preferred_role = 'p') then 1
                                       when hostname = (select hostname from pg_catalog.gp_segment_configuration where content = -1 and preferred_role = 'm') then 2
                                       else 3
                                   end asc
                             , hostname asc, filesystem asc " >> ${LOG_FILE}
else
    # GPDB 4.x/5.x
    echo >> ${LOG_FILE}
    echo "-- log alert panic history" | tee -a ${LOG_FILE}
    psql gpperfmon -c " select logtime
                             , loguser
                             , logdatabase
                             , logpid
                             , loghost
                             , logseverity
                             , logstate
                             , substring(logmessage, 1, 1000) as logmessage
                             , substring(logdetail, 1, 1000) as logdetail
                             , substring(loghint, 1, 1000) as loghint
                             , substring(logquery, 1, 1000) as logquery
                             , logquerypos
                             , substring(logcontext, 1, 1000) as logcontext
                             , substring(logdebug, 1, 1000) as logdebug
                             , logcursorpos
                             , logfile
                             , logline
                             , substring(logstack, 1, 500) as logstack
                          from public.log_alert_history
                         where logtime > current_date - 7
                           and logseverity = 'PANIC'
                         order by logtime desc
                         limit 100 " >> ${LOG_FILE}

    echo >> ${LOG_FILE}
    echo "-- log alert history" | tee -a ${LOG_FILE}
    psql gpperfmon -c " select logtime
                             , loguser
                             , logdatabase
                             , logpid
                             , loghost
                             , logseverity
                             , logstate 
                             , substring(logmessage, 1, 1000) as logmessage
                             , substring(logdetail, 1, 1000) as logdetail 
                             , substring(loghint, 1, 1000) as loghint
                             , substring(logquery, 1, 1000) as logquery
                             , logquerypos 
                             , substring(logcontext, 1, 1000) as logcontext
                             , substring(logdebug, 1, 1000) as logdebug
                             , logcursorpos 
                             , logfile
                             , logline 
                             , substring(logstack, 1, 500) as logstack
                          from public.log_alert_history
                         where logtime > current_date - 7
                           and logseverity not in ('INFO', 'PANIC','WARNING')
                           and logstate not like '42%'
                           and logstate not in ('57014', '57P01', '08006', '3D000', '3F000', '22P06')
			   and logmessage not like 'current transaction is aborted%'
			   and logmessage not like 'password authentication failed%'
   			   and logmessage not like 'no pg_hba.conf%'
                         order by logtime desc
                         limit 5000 " >> ${LOG_FILE}

    echo >> ${LOG_FILE}
    echo "-- node size" | tee -a ${LOG_FILE}
    psql gpperfmon -c "select ctime, hostname, filesystem, pg_size_pretty(total_bytes) as total, pg_size_pretty(bytes_used) as used, pg_size_pretty(bytes_available) as available
                            , round(bytes_used::numeric / total_bytes::numeric * 100, 2) as used_percent
                         from public.diskspace_history
                        where ctime = (select max(ctime) from public.diskspace_history)
                          and filesystem not like '/boot%'
                        order by case
                                      when hostname = (select hostname from pg_catalog.gp_segment_configuration where content = -1 and preferred_role = 'p') then 1
                                      when hostname = (select hostname from pg_catalog.gp_segment_configuration where content = -1 and preferred_role = 'm') then 2
                                      else 3
                                 end asc
                               , hostname asc, filesystem asc " >> ${LOG_FILE}
fi

echo >> ${LOG_FILE}
echo "-- db status history" | tee -a ${LOG_FILE}
psql gpperfmon -c "select * from pg_catalog.gp_configuration_history where time > current_date -30;"  >> ${LOG_FILE}
 
echo >> ${LOG_FILE}
echo "-- db size" | tee -a ${LOG_FILE}
psql gpperfmon -c " select sodddatname as database_name
                         , pg_catalog.pg_size_pretty(sodddatsize) as database_size
                      from gp_toolkit.gp_size_of_database
                     order by sodddatsize desc " >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo "-- master size" | tee -a ${LOG_FILE}
du -sh $MASTER_DATA_DIRECTORY >> ${LOG_FILE}

echo >> ${LOG_FILE}
echo "-- pglog" >> ${LOG_FILE}
du -sh $MASTER_DATA_DIRECTORY/pg_log >> ${LOG_FILE}
echo >> ${LOG_FILE}
echo "-- pglog head -n 10" >> ${LOG_FILE}
ls -alrth $MASTER_DATA_DIRECTORY/pg_log/gpdb-*.csv | head -n 10 >> ${LOG_FILE}
echo >> ${LOG_FILE}
echo "-- pglog tail -n 10" >> ${LOG_FILE}
ls -alrth $MASTER_DATA_DIRECTORY/pg_log/gpdb-*.csv | tail -n 10 >> ${LOG_FILE}

if [ ! -d "$STAT_LOG_DIR" ]; then
    echo >> ${LOG_FILE}
    echo 'statlog directory not found!!!' | tee -a $LOG_PATH
else
    echo >> ${LOG_FILE}
    echo "-- qq" >> ${LOG_FILE}
    cd $STAT_LOG_DIR
    ls -la qq* | tail -10 >> ${LOG_FILE}

    echo >> ${LOG_FILE}
    echo "-- locks" >> ${LOG_FILE}
    cd $STAT_LOG_DIR
    ls -la lt* | tail -10 >> ${LOG_FILE}
fi

echo >> ${LOG_FILE}
echo "-- admin log" >> ${LOG_FILE}
cd ~/gpAdminLogs
for i in {0..14}
do
    DAY_AGO=$(date -d "-$i day" +"%Y%m%d")
    #echo $DAY_AGO >> ${LOG_FILE}
    egrep "ERROR|FATAL|WARNING" *"$DAY_AGO"* >> ${LOG_FILE}
done

DB_LIST=`psql gpperfmon -t -c "select datname from pg_catalog.pg_database where datname not like 'template%' and datname <> 'gpperfmon'"`

echo >> ${LOG_FILE}
for DB in $DB_LIST
do
    #echo "DB_NAME=$DB"
    echo "-- [$DB] catalog size" | tee -a ${LOG_FILE}
    psql $DB -c " select current_database() as "database", relname as "relation", relkind, pg_size_pretty(pg_relation_size(C.oid)) as "size"
                    from pg_class c
                    left join pg_namespace n on (n.oid = c.relnamespace)
                   where nspname in ('pg_catalog')
                     and relkind = 'r'
                   order by pg_relation_size(c.oid) desc, relname limit 20;" >> ${LOG_FILE}
done
 
echo >> ${LOG_FILE}
for DB in $DB_LIST
do
    echo "-- [$DB] catalog index size" | tee -a ${LOG_FILE}
    psql $DB -c "select current_database() as "database", relname as "relation", relkind, pg_size_pretty(pg_relation_size(C.oid)) as "size"
                   from pg_class c
                   left join pg_namespace n on (n.oid = c.relnamespace)
                  where nspname in ('pg_catalog')
                    and relkind = 'i'
                  order by pg_relation_size(c.oid) desc, relname limit 20;" >> ${LOG_FILE}
done

DB_ALL_LIST=`psql gpperfmon -tc "select datname from pg_catalog.pg_database WHERE datname NOT like 'template%'"`

echo >> ${LOG_FILE}
echo "-- database age" | tee -a ${LOG_FILE}
psql gpperfmon -c "SELECT datname, datfrozenxid, to_char(age(datfrozenxid), '999,999,999,999') as age, age(datfrozenxid)::numeric/500000000*100::numeric(11,2) as age  FROM pg_database WHERE datname NOT like 'template%';" >> ${LOG_FILE}

echo >> ${LOG_FILE}
for DB in $DB_ALL_LIST
do
    echo "-- [$DB] table master age" | tee -a ${LOG_FILE}
    psql $DB -c "select current_database() as database
                      , n.nspname
                      , c.relname
                      , c.relstorage
                      , to_char(age(c.relfrozenxid), '999,999,999,999') as age
                   from pg_class c
                   join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                  where c.relkind ='r'
                    and c.relstorage not in ('x', 'a', 'c')
                    and age(c.relfrozenxid) > 400000
                    and n.nspname != 'information_schema'
                    and not (n.nspname = 'pg_catalog' and c.relname in ('gp_global_sequence', 'gp_persistent_relation_node', 'gp_persistent_database_node', 'gp_persistent_tablespace_node', 'gp_persistent_filespace_node'))
                  order by age(c.relfrozenxid) desc
                  limit 20" >> ${LOG_FILE}

    echo "-- [$DB] table segment age" | tee -a ${LOG_FILE}

    psql $DB -c "WITH cluster AS (
			SELECT gp_segment_id, datname, age(datfrozenxid) age FROM pg_database
			UNION ALL
			--SELECT gp_segment_id, datname, age(datfrozenxid) age FROM gp_dist_random('pg_database')
	SELECT gp_segment_id, datname, age(datfrozenxid) age FROM gp_dist_random('pg_database')	)
			SELECT  gp_segment_id, datname, age,
        			CASE
                			WHEN age < (2^31-1 - current_setting('xid_stop_limit')::int - current_setting('xid_warn_limit')::int) THEN 'BELOW WARN LIMIT'
                			WHEN  ((2^31-1 - current_setting('xid_stop_limit')::int - current_setting('xid_warn_limit')::int) < age) AND (age <  (2^31-1 - current_setting('xid_stop_limit')::int)) THEN 'OVER WARN LIMIT and UNDER STOP LIMIT'
                			WHEN age > (2^31-1 - current_setting('xid_stop_limit')::int ) THEN 'OVER STOP LIMIT'
                			WHEN age < 0 THEN 'OVER WRAPAROUND'
        			END
			FROM cluster
			WHERE datname NOT like 'template%'
			ORDER BY datname, gp_segment_id
                  	limit 20" >> ${LOG_FILE}

    psql $DB -c "select current_database() as database
                      , c.gp_segment_id
                      , n.nspname
                      , c.relname
                      , c.relstorage
                      , to_char(age(c.relfrozenxid), '999,999,999,999') as age
                   from gp_dist_random('pg_class') c
                   join pg_catalog.pg_namespace n on n.oid = c.relnamespace
                  where c.relkind ='r'
                    and c.relstorage not in ('x', 'a', 'c')
                    and age(c.relfrozenxid) > 40000000
                    and n.nspname != 'information_schema'
                    and not (n.nspname = 'pg_catalog' and c.relname in ('gp_global_sequence', 'gp_persistent_relation_node', 'gp_persistent_database_node', 'gp_persistent_tablespace_node', 'gp_persistent_filespace_node'))
                  order by age(c.relfrozenxid) desc
                  limit 20" >> ${LOG_FILE}

    echo >> ${LOG_FILE}
done

echo >> ${LOG_FILE}
echo "-- bloat diag" | tee -a ${LOG_FILE}
for DB in $DB_ALL_LIST
do
    echo "-- [$DB] bloat diag" | tee -a ${LOG_FILE}
    psql $DB -c "select current_database() as database_name
                      , bdirelid
                      , bdinspname as schema_name
                      , bdirelname as table_name
                      , bdirelpages
                      , bdiexppages
                      , bdidiag 
                   from gp_toolkit.gp_bloat_diag
                  limit 50" >> ${LOG_FILE}

    echo >> ${LOG_FILE}
done
 
echo "$EXEC : END DATE : `date "+%Y-%m-%d %H:%M:%S"`" | tee -a ${LOG_FILE}



