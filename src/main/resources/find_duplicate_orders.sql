insert into orders
select o.* from (
  select max(id) as id
    from orders_raw
    where system_id=?
    group by region_id, system_id, station_id, type_id, bid, price, volume
    having count(*) > 1
  union
  select max(id) as id
    from orders_raw
    where system_id=?
    group by region_id, system_id, station_id, type_id, bid, price, volume
    having count(*) = 1) v
  join orders_raw o on o.id = v.id;