INSERT INTO profit_mv
SELECT
		s.type_id as id, 
		s.region_id,
		s.system_id,
		b.avg_price as avg_buy,
		b.min_price as min_buy,
		b.max_price as max_buy,
		s.avg_price as avg_sell,
		s.min_price as min_sell,
		s.max_price as max_sell,
		s.volume as sell_volume,
		b.volume as buy_volume
	FROM (
		SELECT 
			type_id,
      region_id,
      system_id,
      sum(price*volume)/sum(volume) as avg_price,
      max(price) as max_price,
      min(price) as min_price,
      sum(volume) as volume
		FROM market.orders 
		WHERE system_id = ? and bid = 0
		GROUP BY region_id, system_id, type_id) s
	JOIN (
		SELECT
		  type_id,
      region_id,
      system_id,
      sum(price*volume)/sum(volume) as avg_price,
      max(price) as max_price,
      min(price) as min_price,
      sum(volume) as volume
		FROM market.orders m
		WHERE system_id = ? and bid = 1
		GROUP BY region_id, system_id, type_id) b
	ON b.type_id = s.type_id and b.region_id = s.region_id and b.system_id = s.system_id 