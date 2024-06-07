
fact_market_data = '''
    INSERT INTO prod.fact_market_data (
        symbol_id
        , units
        , price
        , value
        , change 
        , percent_change
        , contract
        , high
        , low
        ,time_edt
    )
    select * from (
select distinct b.id as symbol_id, a.units, a.price,0 as value, a.change, a.percent_change, a.contract,0 as high,0 as low, a.time_edt 
FROM dev.agriculture a join prod.dim_symbols b on a.symbol = b.name and b.cat_id= 1
union
select distinct b.id as symbol_id, a.units, a.price,0 as value, a.change, a.percent_change, a.contract,0 as high,0 as low, a.time_edt 
FROM dev.metal a join prod.dim_symbols b on a.symbol = b.name and b.cat_id= 4
union
select distinct b.id as symbol_id, a.units, a.price,0 as value, a.change, a.percent_change, a.contract,0 as high,0 as low, a.time_edt 
FROM dev.energy a join prod.dim_symbols b on a.symbol = b.name and b.cat_id= 3
union
select distinct b.id as symbol_id, '' as units, 0 as price,a.value, a.change, a.percent_change, '' as contract,a.high,a.low, a.time_edt 
FROM dev.commodities a join prod.dim_symbols b on a.symbol = b.name and b.cat_id = 2
) as x
order by symbol_id;
'''


dim_categories = '''
    INSERT INTO prod.dim_categories (name)
    VALUES ('agriculture'),('commodities'),('energy'),('metal');

'''

dim_symbols = '''
    INSERT INTO prod.dim_symbols (cat_id, name)
    select cat_id,symbol from 
(select (select id from prod.dim_categories where name = 'agriculture') as cat_id,a.symbol from dev.agriculture a 
union
select (select id from prod.dim_categories where name = 'metal') as cat_id,b.symbol from dev.metal b 
union
select (select id from prod.dim_categories where name = 'energy') as cat_id,c.symbol from dev.energy c
union
select (select id from prod.dim_categories where name = 'commodities') as cat_id,d.symbol from dev.commodities d) e
order by cat_id,symbol;
'''

dim_dates = '''
    INSERT INTO prod.dim_dates (
        date
        , year
        , month
        , day
        , quarter
        , is_weekend
    )
     SELECT distinct a.submission_date, EXTRACT(YEAR FROM a.submission_date),
	 EXTRACT(MONTH FROM a.submission_date), EXTRACT(DAY FROM a.submission_date), 
	 EXTRACT(QUARTER FROM a.submission_date),
	 CASE WHEN EXTRACT(DOW FROM a.submission_date) IN (6,0) THEN true ELSE false END AS is_weekend
    FROM dev.agriculture a;
'''

transformation_queries = [dim_categories,dim_symbols,dim_dates,fact_market_data]