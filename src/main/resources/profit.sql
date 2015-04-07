INSERT INTO profit_mv
SELECT
		s.type_id as id, 
		s.region_id,
		s.system_id,
		b.price as buy, 
		s.price as sell, 
		s.volume as sell_volume,
		b.volume as buy_volume
	FROM (
		SELECT 
			type_id, 
            region_id, 
            system_id, 
            max(price) as price, 
            sum(volume) as volume
		FROM market.orders 
		WHERE region_id = ? and system_id = ? and bid = 0
		GROUP BY region_id, system_id, type_id) s
	JOIN (
		SELECT 
			type_id, 
            region_id, 
            system_id, 
            max(price) as price, 
            sum(volume) as volume
		FROM market.orders m
		WHERE region_id = ? and system_id = ? and bid = 1
		GROUP BY region_id, system_id, type_id) b
	ON b.type_id = s.type_id and b.region_id = s.region_id and b.system_id = s.system_id 