---------- CRIANDO TABELAS ----------



---------- CRIANDO POLÍTICA DE COMPRESSÃO ----------



---------- CRIANDO DATA MARTS ----------

-- mart 'mart_recursos_hardware'

create materialized view mart_recursos_hardware
	with(timescaledb.continuous) as
	select 
		time_bucket('20 seconds', ts) as bucket,
		satellite_id,
		avg((payload->'diagnostics'->>'cpu_usage_pct')::numeric) as avg_cpu_use,
		max((payload->'diagnostics'->>'cpu_usage_pct')::numeric) as max_cpu_use,
		avg((payload->'diagnostics'->>'memory_usage_mb')::numeric) as avg_mem_use,
		max((payload->'diagnostics'->>'memory_usage_mb')::numeric) as max_mem_use,
		count(*) filter (where(payload->'diagnostics'->>'last_error_code')::int != 0) as total_errors
	from satellite_data
	group by bucket, satellite_id;

-- cont agg 'mart_recursos_hardware'
select add_continuous_aggregate_policy('mart_recursos_hardware',
	start_offset => interval '1 hour',
	end_offset => interval '2 seconds',
	schedule_interval => interval '20 seconds'
)

