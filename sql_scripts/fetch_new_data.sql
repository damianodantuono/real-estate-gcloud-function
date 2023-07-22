SELECT Title, CONCAT(CAST(price as string), ' EUR/MESE') as price , link
FROM `real-estate-analyser-dv.dwhre.FCT_HOUSES` f
inner join `real-estate-analyser-dv.dwhre.DIM_CITY` d on d.city_id = f.city_id
where f.INSERT_UPDATE_TIMESTAMP >= COALESCE((SELECT last_run_start FROM `real-estate-analyser-dv.config.notifier_runner`), '1900-01-01 00:00:00')
and not deleted_flag
and price <= 1000
and city_name = 'como'
and IS_RENT
;