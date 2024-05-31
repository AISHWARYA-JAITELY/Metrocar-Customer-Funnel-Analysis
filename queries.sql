/* 
Query 1: 
<Creating multiple temporary table to do further aggregations and selecting only the data that I need>
*/

SELECT 1; -- with app as
(select
0 as Funnel_step,
'App Downloads' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
download_ts::date AS download_date,
count (distinct app_download_key) as User_count,
count( ride_requests.ride_id) filter(where cancel_ts is null and request_ts < download_ts) as ride_count
From app_downloads
full join signups on app_downloads.app_download_key=signups.session_id
full join ride_requests on signups.user_id=ride_requests.user_id
group by 1,2,3,4,5),
 
 
signup as (
select
1 as Funnel_step,
'Sign Ups' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
--signup_ts::date AS extracted_date,
download_ts::date AS download_date,
count (distinct signups.user_id) as User_count,
count( ride_requests.ride_id) filter(where cancel_ts is null and request_ts < signup_ts) as ride_count
from signups
full join app_downloads on app_downloads.app_download_key=signups.session_id
full join ride_requests on signups.user_id=ride_requests.user_id
group by 1,2,3,4,5),


requested_ride as (
select
2 as Funnel_step,
'Requested Ride' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
download_ts::date AS download_date,
count (distinct ride_requests.user_id) as User_count,
count( ride_requests.ride_id)  as ride_count
from ride_requests
full join signups on ride_requests.user_id=signups.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
group by 1,2,3,4,5),


accepted_ride as(
select
3 as Funnel_step,
'Accepted Ride' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
download_ts::date AS download_date,
count( ride_requests.user_id) filter (where ride_requests.accept_ts is not Null) as User_count,
count( ride_requests.ride_id)filter (where ride_requests.accept_ts is not Null)  as ride_count
from ride_requests
full join signups on ride_requests.user_id=signups.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
group by 1,2,3,4,5),


completed_ride as(
select
4 as Funnel_step,
'Completed Ride' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
download_ts::date AS download_date,
count( ride_requests.user_id) filter (where ride_requests.dropoff_ts is not Null) as User_count,
count( ride_requests.ride_id)filter (where ride_requests.dropoff_ts is not Null)  as ride_count
from ride_requests
full join signups on ride_requests.user_id=signups.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
group by 1,2,3,4,5),


paid_ride as(
select
5 as Funnel_step,
'Paid Ride' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
download_ts::date AS download_date,
count( ride_requests.user_id) filter (where ride_requests.dropoff_ts is not Null and transactions.charge_status ='Approved') as User_count,
count( ride_requests.ride_id)filter (where ride_requests.dropoff_ts is not Null and transactions.charge_status ='Approved')  as ride_count
from transactions
full join ride_requests on transactions.ride_id=ride_requests.ride_id
full join signups on ride_requests.user_id=signups.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
group by 1,2,3,4,5),


reviews as (
select
6 as Funnel_step,
'Reviews' as Funnel_name,
coalesce(age_range,'Unknown')as Age_range,
platform as Platform,
download_ts::date AS download_date,
count(distinct reviews.user_id) as User_count,
count( reviews.ride_id) as ride_count
from reviews
full join signups on signups.user_id=reviews.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
group by 1,2,3,4,5)






/* 
Query 2: 
<Bring all the created temp tables together so the dataset for tableau's put together >
*/

SELECT 1; -- combined_table as (
select *
from app

UNION ALL

select *
from signup

UNION ALL

select *
from requested_ride

UNION ALL

select *
from accepted_ride

UNION ALL

select *
from completed_ride

UNION ALL

select *
from paid_ride

UNION ALL

select *
from reviews)

select 
*
from combined_table


/* 
Query 3: 
<extracting the hour and frequency of the ride on a given day and the revenue generated to study the patterns of demand>
*/

select 
extract(hour from ride_requests.request_ts) as hour_of_request,
count(ride_requests.ride_id)filter(where accept_ts is not null) as rides_accepted_per_hour,
purchase_amount_usd as revenue
from ride_requests
full join signups on ride_requests.user_id=signups.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
full join transactions on transactions.ride_id=ride_requests.ride_id
group by 1,3
order by 1,3

/* 
Query 4: 
<confirming the count of rides per platform>
*/
SELECT 
COUNT (request_ts),
PLATFORM 
from ride_requests
full join signups on ride_requests.user_id=signups.user_id
full join app_downloads on app_downloads.app_download_key=signups.session_id
group by 2



