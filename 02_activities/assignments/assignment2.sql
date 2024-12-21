/* ASSIGNMENT 2 */
/* SECTION 2 */

--section 1
-- I searched this, Slowly Changing Dimensions (SCD), which define how changes to dimension data (like customer addresses) are handled over time.
--In Type 1, the CUSTOMER_ADDRESS table is updated with the most recent address for each customer.
--No historical records are kept in this table, meaning that only the current address for each customer is available
--In Type 2, the CUSTOMER_ADDRESS table keeps a history of all address changes.
-- Every time a customer changes their address, a new record is created in the table with the updated address, and the previous record is marked as outdated 

-- COALESCE
/* 1. Our favourite manager wants a detailed long list of products, but is afraid of tables! 
We tell them, no problem! We can produce a list with all of the appropriate details. 



Using the following syntax you create our super cool and not at all needy manager a list:

SELECT 
product_name || ', ' || product_size|| ' (' || product_qty_type || ')'
FROM product

But wait! The product table has some bad data (a few NULL values). 
Find the NULLs and then using COALESCE, replace the NULL with a 
blank for the first problem, and 'unit' for the second problem. 

HINT: keep the syntax the same, but edited the correct components with the string. 
The `||` values concatenate the columns into strings. 
Edit the appropriate columns -- you're making two edits -- and the NULL rows will be fixed. 
All the other rows will remain the same.) */

SELECT 
x.product_name || ',' || x.new_product_size || ',' || '(' || x.new_product_qty_type || ')' as combined_format
from (
SELECT product_name, product_size, product_qty_type
,coalesce(nullif(product_size,''),'blank') as new_product_size
,ifnull(product_qty_type, 'unit') as new_product_qty_type
FROM product)x


--Windowed Functions
/* 1. Write a query that selects from the customer_purchases table and numbers each customer’s  
visits to the farmer’s market (labeling each market date with a different number). 
Each customer’s first visit is labeled 1, second visit is labeled 2, etc. 

You can either display all rows in the customer_purchases table, with the counter changing on
each new market date for each customer, or select only the unique market dates per customer 
(without purchase details) and number those visits. 
HINT: One of these approaches uses ROW_NUMBER() and one uses DENSE_RANK(). */

SELECT *
from(
SELECT customer_id, market_date
-- outlining num of visit per customer accounting for same market_date values being assigend as one visit
,DENSE_RANK() OVER(PARTITION by customer_id ORDER BY market_date DESC) as [num_of_visit]
FROM customer_purchases)x


/* 2. Reverse the numbering of the query from a part so each customer’s most recent visit is labeled 1, 
then write another query that uses this one as a subquery (or temp table) and filters the results to 
only the customer’s most recent visit. */

SELECT *
from(
SELECT distinct customer_id, market_date
-- outlining num of visit per customer accounting for same market_date values being assigend as one visit
,DENSE_RANK() OVER(PARTITION by customer_id ORDER BY market_date DESC) as [num_of_visit]
FROM customer_purchases)x
WHERE [num_of_visit] is 1;


/* 3. Using a COUNT() window function, include a value along with each row of the 
customer_purchases table that indicates how many different times that customer has purchased that product_id. */


SELECT 
customer_id,
product_id,
count(x.purchase) as num_of_purchased_time

from(
select *
,row_number()OVER(PARTITION by customer_id ORDER by product_id) as purchase
FROM customer_purchases)x

GROUP by customer_id, product_id;

--another way
SELECT 
  customer_id,
  product_id,
  COUNT(market_date) AS num_of_purchased_times
FROM customer_purchases
GROUP BY customer_id, product_id;

-- String manipulations
/* 1. Some product names in the product table have descriptions like "Jar" or "Organic". 
These are separated from the product name with a hyphen. 
Create a column using SUBSTR (and a couple of other commands) that captures these, but is otherwise NULL. 
Remove any trailing or leading whitespaces. Don't just use a case statement for each product! 

| product_name               | description |
|----------------------------|-------------|
| Habanero Peppers - Organic | Organic     |

Hint: you might need to use INSTR(product_name,'-') to find the hyphens. INSTR will help split the column. */

SELECT product_id, product_name, product_size, product_category_id, product_qty_type
,nullif(substr(product_name,instr(product_name, '-')+2, instr(product_name, '-')),'') as description
FROM product

--from (
--SELECT *
--,instr(product_name, '-')
--,substr(product_name,instr(product_name, '-')+2, instr(product_name, '-')) as cleaning
--from product
--)x



/* 2. Filter the query to show any product_size value that contain a number with REGEXP. */

-- highest_sale or "best day"
DROP TABLE if EXISTS temp.highest_sales;
CREATE TEMP TABLE temp.highest_sales AS

SELECT product_id, market_date, x.counts
,dense_rank()OVER(order by counts DESC) as [sales_ranks]
from(
select product_id, market_date, customer_id, quantity, cost_to_customer_per_qty
, sum(quantity * cost_to_customer_per_qty) as counts
from customer_purchases
GROUP by market_date)x;


-- lowest sales or "worst day" 
DROP TABLE if EXISTS temp.lowest_sales;
CREATE TEMP TABLE temp.lowest_sales AS

SELECT product_id, market_date, x.counts
,dense_rank()OVER(order by counts ASC) as [sales_ranks]
from(
select product_id, market_date, customer_id, quantity, cost_to_customer_per_qty
, sum(quantity * cost_to_customer_per_qty) as counts
from customer_purchases
GROUP by market_date)x;


-- Union the results 
SELECT *
from temp.highest_sales
WHERE sales_ranks = 1 

UNION

SELECT *
from temp.lowest_sales
WHERE sales_ranks = 1;



-- UNION
/* 1. Using a UNION, write a query that displays the market dates with the highest and lowest total sales.

HINT: There are a possibly a few ways to do this query, but if you're struggling, try the following: 
1) Create a CTE/Temp Table to find sales values grouped dates; 
2) Create another CTE/Temp table with a rank windowed function on the previous query to create 
"best day" and "worst day"; 
3) Query the second temp table twice, once for the best day, once for the worst day, 
with a UNION binding them. */




/* SECTION 3 */

-- Cross Join
/*1. Suppose every vendor in the `vendor_inventory` table had 5 of each of their products to sell to **every** 
customer on record. How much money would each vendor make per product? 
Show this by vendor_name and product name, rather than using the IDs.

HINT: Be sure you select only relevant columns and rows. 
Remember, CROSS JOIN will explode your table rows, so CROSS JOIN should likely be a subquery. 
Think a bit about the row counts: how many distinct vendors, product names are there (x)?
How many customers are there (y). 
Before your final group by you should have the product of those two queries (x*y).  */

-- product name and vendor name tables
DROP TABLE IF EXISTS temp.x;

CREATE TEMP TABLE temp.x AS

SELECT distinct vendor_name, product_name, customer_id,
row_number() OVER (PARTITION by vendor_name, customer_id ORDER BY product_name ASC) as row_num

from vendor_inventory vi
INNER JOIN customer_purchases cp
	ON cp.product_id = vi.product_id
INNER JOIN vendor v
	ON v.vendor_id = vi.vendor_id
Inner JOIN product p
	ON 	p.product_id = vi.product_id;

-- 5 qty revenue
DROP TABLE IF EXISTS temp.rev;

CREATE temp TABLE IF NOT EXISTS temp.rev AS
SELECT distinct product_name, customer_id, sum(5 * original_price) as vendor_revenue
from vendor_inventory vi
INNER JOIN customer_purchases cp
	ON vi.product_id = cp.product_id
Inner JOIN product p
	ON p.product_id = vi.product_id
GROUP by product_name, customer_id
ORDER BY product_name ASC;
--

-- cross join
SELECT x.product_name, x.vendor_name, x.customer_id, rev.vendor_revenue
FROM temp.x AS x
CROSS JOIN temp.rev


-- INSERT
/*1.  Create a new table "product_units". 
This table will contain only products where the `product_qty_type = 'unit'`. 
It should use all of the columns from the product table, as well as a new column for the `CURRENT_TIMESTAMP`.  
Name the timestamp column `snapshot_timestamp`. */
-- TEMP table
DROP TABLE IF EXISTS temp.product_unit;


CREATE temp TABLE IF NOT EXISTS temp.product_unit AS

SELECT *,
CURRENT_TIMESTAMP as snapshot_timestamp
FROM product
where product_qty_type = 'unit'
ORDER by product_id ASC;




/*2. Using `INSERT`, add a new row to the product_units table (with an updated timestamp). 
This can be any product you desire (e.g. add another record for Apple Pie). */

-- insert a new row
INSERT INTO temp.product_unit
(product_id, product_name, product_size, product_category_id, product_qty_type, snapshot_timestamp)
VALUES(24, 'chocholate', '10 kg', 3, 'kg', DATETIME(CURRENT_TIMESTAMP, '+1 day'));

-- DELETE
/* 1. Delete the older record for the whatever product you added. 

HINT: If you don't specify a WHERE clause, you are going to have a bad time.*/

delete from temp.product_unit 
where product_id = 24


-- UPDATE
/* 1.We want to add the current_quantity to the product_units table. 
First, add a new column, current_quantity to the table using the following syntax.

ALTER TABLE product_units
ADD current_quantity INT;

Then, using UPDATE, change the current_quantity equal to the last quantity value from the vendor_inventory details.

HINT: This one is pretty hard. 
First, determine how to get the "last" quantity per product. 
Second, coalesce null values to 0 (if you don't have null values, figure out how to rearrange your query so you do.) 
Third, SET current_quantity = (...your select statement...), remembering that WHERE can only accommodate one column. 
Finally, make sure you have a WHERE statement to update the right row, 
	you'll need to use product_units.product_id to refer to the correct row within the product_units table. 
When you have all of these components, you can run the update statement. */


-- last market_date
SELECT product_id, quantity
FROM vendor_inventory
WHERE (product_id, market_date) IN (
    SELECT product_id, MAX(market_date)
    FROM vendor_inventory
    GROUP BY product_id
)
ORDER BY product_id;
-- current_quantity

ALTER TABLE product_unit
ADD current_quantity INT;

-- SET

UPDATE product_unit
SET current_quantity = (
    SELECT quantity
    FROM vendor_inventory
    WHERE product_unit.product_id = vendor_inventory.product_id
    LIMIT 1
);
-- removing null

SELECT *
, coalesce(current_quantity, 0)
FROM product_unit