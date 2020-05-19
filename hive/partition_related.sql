set hive.session.id="job_name@wangpei";

-- drop
alter table database.table_name drop partition (partition_name='${partition_value}');

-- add
alter table database.table_name add partition (partition_name='${partition_value}') location '${partition_path}';
