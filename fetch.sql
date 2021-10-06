-- What are the top 5 brands by receipts scanned for most recent month?
select b.id brand_id, b.name brand_name, 
	extract(year_month from cast(r.date_scanned as date)) as month, 
    count(r.id) as no_of_receipts_scanned
from receipts r
join rewardsreceiptitemlist i on r.id = i.id
join brands b on i.barcode = b.barcode
where extract(year_month from cast(r.date_scanned as date)) = (
				select extract(year_month from cast(r.date_scanned as date)) as y_m
				from receipts r
				join rewardsreceiptitemlist i on r.id = i.id
				join brands b on i.barcode = b.barcode
                order by y_m desc 
                limit 1
)
group by b.id, b.name, month 
order by 4 desc;
-- How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
 -- no data for previous month (after joining the tables, there is data for only jan 2021 month. Rest all the data is null values)
 
-- When considering average spend from receipts with 
	-- 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
SELECT r.rewards_receipt_status, round(avg(r.total_spent),2) as avg_spent
FROM fetchdb.receipts r
group by r.rewards_receipt_status
order by avg_spent desc;

-- When considering total number of items purchased from 
	-- receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
SELECT r.rewards_receipt_status, sum(r.purchased_item_count) as number_of_items
FROM fetchdb.receipts r
group by r.rewards_receipt_status
order by number_of_items desc;

-- Which brand has the most spend among users who were created within the past 6 months?
select b.id, b.name, round(sum(r.total_spent),2) as tot_spend
from users u
join receipts r on u.id = r.user_id
join rewardsreceiptitemlist i on r.id = i.id
join brands b on i.barcode = b.barcode
where TIMESTAMPDIFF(MONTH, cast(r.date_scanned as date), (select max(cast(r.date_scanned as date))
																	from receipts r)) <=6
group by b.id, b.name
order by 3 desc;


-- Which brand has the most transactions among users who were created within the past 6 months?
select b.id, b.name, round(count(r.id),2) as tot_transactions
from users u
join receipts r on u.id = r.user_id
join rewardsreceiptitemlist i on r.id = i.id
join brands b on i.barcode = b.barcode
where TIMESTAMPDIFF(MONTH, cast(r.date_scanned as date), (select max(cast(r.date_scanned as date))
																	from receipts r)) <=6
group by b.id, b.name
order by 3 desc;
-- assuming every receipt as transaction