-- Run evidence: one Iceberg snapshot per Silver commit (run each statement separately in Athena).
SELECT committed_at, snapshot_id, operation,
       summary['added-records'] AS added_records,
       summary['deleted-records'] AS deleted_records,
       summary['changed-partition-count'] AS changed_partitions
FROM nyc_taxi."trips_clean$snapshots"
ORDER BY committed_at DESC;

SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor
FROM nyc_taxi."trips_clean$history"
ORDER BY made_current_at DESC;
