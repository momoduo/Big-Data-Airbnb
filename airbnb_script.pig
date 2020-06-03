-- STEP 1: Running pig script in grunt shell
-- A: load lalistings and sf_listings, remember to change username

lalistings = LOAD '/user/ychen148/airbnb/la/la_listings.csv' USING PigStorage(',') AS (
id:chararray,
name:chararray,
host_id:chararray,
host_name:chararray,
neighborhood:chararray,
latitude:double,
longitude:double, 
room_type:chararray, 
price:double,
minimum_nights:int, 
number_of_reviews:int, 
last_review:datetime,
reviews_per_month:double);

sflistings = LOAD '/user/ychen148/airbnb/sf/sf_listings.csv' USING PigStorage(',') AS (
id:chararray,
name:chararray,
host_id:chararray,
host_name:chararray,
neighborhood:chararray,
latitude:double,
longitude:double, 
room_type:chararray, 
price:double,
minimum_nights:int, 
number_of_reviews:int, 
last_review:datetime,
reviews_per_month:double);

-- B: clean la_listings.csv and sf_listings.csv

-- B.1: Get rid of records that do not have reviews_per_month by using FILTER:

lahasreviews = FILTER lalistings BY reviews_per_month IS NOT NULL;

sfhasreviews = FILTER sflistings BY reviews_per_month IS NOT NULL;

-- B.2 Get rid of records that do not have location data (longitude and latitude)

lahaslongitude = FILTER lahasreviews BY longitude IS NOT NULL;

lahaslatitude = FILTER lahaslongitude BY latitude IS NOT NULL;

sfhaslongitude = FILTER sfhasreviews BY longitude IS NOT NULL;

sfhaslatitude = FILTER sfhaslongitude BY latitude IS NOT NULL;


-- C: create a new relation with total price, including 14% tax for LA rent

lanew_listings = FOREACH lahaslatitude GENERATE neighborhood, id, price + (price * 0.14) AS finalprice:double, latitude, longitude, room_type, reviews_per_month;

-- D: Group the neighborhood by finalprice. 

-- D.1: Create a new relation with only neighborhood and finalprice from lanew_listings table.

laprice = FOREACH lanew_listings GENERATE finalprice, neighborhood;

-- D.2: Group price by neighborhood and see results.

lapricebyneighborhood = GROUP laprice BY neighborhood;


-- D.3: Calculate the average price of each region to compare the most expensive/cheap place to live.

latotals = FOREACH lapricebyneighborhood GENERATE group, AVG(laprice.finalprice) AS lafinalprice;

 -- D.4: Sort the data by price and see the top 15 most expensive places to live

lasortedpricedesc = ORDER latotals BY lafinalprice DESC;

latop15 = LIMIT lasortedpricedesc 15;

dump latop15;

-- Store sortedpricedesc into airbnb/la directory.

store lasortedpricedesc INTO 'airbnb/la/sorted_avg_price' USING PigStorage(',');


-- A: Now we are going to repeat steps 3D - 3E for SF using the following commands:
--create a new relation with total price, including 14% tax for SF Rental

sfnew_listings = FOREACH sfhaslatitude GENERATE neighborhood, id, price + (price * 0.14) AS finalprice:double, latitude, longitude, room_type, reviews_per_month;

--B: Group the neighborhood by finalprice.
-- Create a new relation with only neighborhood and finalprice from sfnew_listings table.

sfprice = FOREACH sfnew_listings GENERATE finalprice, neighborhood;

-- Group price by neighborhood and see results.
sfpricebyneighborhood = GROUP sfprice BY neighborhood;

-- Calculate the average price of each region to compare the most expensive/cheap place to live.

sftotals = FOREACH sfpricebyneighborhood GENERATE group, AVG(sfprice.finalprice) AS sffinalprice;

-- Sort the data by price and see the top 10 most expensive places to live.

sfsortedpricedesc = ORDER sftotals BY sffinalprice DESC;

sftop15 = LIMIT sfsortedpricedesc 15;

dump sftop15;


--Store sfsortedpricedesc into airbnb/la directory.

store sfsortedpricedesc INTO 'airbnb/sf/sorted_avg_price' USING PigStorage(',');


--STEP 2: clean lareviews and sfreviews and perform JOIN

--Load la_reviews.csv files with schema and describe the schema to double check
lareviews = LOAD '/user/ychen148/airbnb/la/la_reviews.csv' USING PigStorage(',') AS (
date:datetime, 
listing_id:chararray, 
reviewer_id:chararray, 
reviewer_name:chararray);

sfreviews = LOAD '/user/ychen148/airbnb/sf/sf_reviews.csv' USING PigStorage(',') AS (
date:datetime, 
listing_id:chararray, 
reviewer_id:chararray, 
reviewer_name:chararray);

--Join lanew_listings with lareviews.
lajoined = JOIN lanew_listings by id, lareviews by listing_id;

--Clean schema and describe to double check.
lacleaned = FOREACH lajoined GENERATE lanew_listings::neighborhood,
lanew_listings::id, 
lanew_listings::finalprice,
lanew_listings::latitude,
lanew_listings::longitude, 
lanew_listings::room_type,
lanew_listings::reviews_per_month, 
lareviews::date, 
lareviews::reviewer_id,
lareviews::reviewer_name;

dump lacleaned;

--Store lacleaned into correct directory and download the file using sftp.
store lacleaned INTO 'airbnb/la/lajoined' USING PigStorage(',');


--Join sfnew_listings with sfreviews.
sfjoined = JOIN sfnew_listings by id, sfreviews by listing_id;

--Clean schema and describe to double check.

sfcleaned = FOREACH sfjoined GENERATE sfnew_listings::neighborhood,
sfnew_listings::id, 
sfnew_listings::finalprice,
sfnew_listings::latitude,
sfnew_listings::longitude, 
sfnew_listings::room_type,
sfnew_listings::reviews_per_month, 
sfreviews::date, 
sfreviews::reviewer_id,
sfreviews::reviewer_name;

dump sfcleaned;


--Store sfcleaned into correct directory and download the file using sftp.

store sfcleaned INTO 'airbnb/sf/sfjoined' USING PigStorage(',');




