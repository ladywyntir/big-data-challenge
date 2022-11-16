# big-data-challenge
Module 22 Challenge - Big Data

### Background
In this assignment, you will put your ETL skills to the test. Many of Amazon's shoppers depend on product reviews to make a purchase. Amazon makes these datasets publicly available. They are quite large and can exceed the capacity of local machines. One dataset alone contains over 1.5 million rows; with over 40 datasets, data analysis can be very demanding on the average local computer. Your first goal will be to perform the ETL process completely in the cloud and upload a DataFrame to an RDS instance. The second goal will be to use PySpark or SQL to perform a statistical analysis of selected data.

This Challenge contains two levels. The second level is optional but highly recommended.
1. Create DataFrames to match production-ready tables from two big Amazon customer review datasets.
2. Analyze whether reviews from Amazon's Vine program are trustworthy.
<hr>

### Setup AWS Database
1. Created a free database in AWS
2. Made sure to make it Public Access and set up the Inbound Security rules to PostgreSQL for all IPv4 and IPv6 addresses. 
    * this took a while to figure out as my connection kept timing out
    * I had to return to our class Zoom recording to watch Dr. A configure this part in our Big Data lessons
<hr>    

### Setup pgAdmin Database Connection
1. Registered a new server
2. Used the AWS Endpoint, Port, Username and Password to connect
3. Used the schema.sql file to add the tables to my new DB
![Image](/Images/postgresql%20DB%20schema%20for%20tables1.jpg)
<hr>

### Code Theory - Level 1
1. Create a new notebook in Google Colab Notebook
2. Select two datasets
    * https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Video_Games_v1_00.tsv.gz
    * https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Video_v1_00.tsv.gz
3. For the First Dataset: amazon_reviews_us_Video_Games_v1_00.tsv.gz: Set up Spark with the latest version
4. Installed Java, Spark and Findspark
5. Set the OS Environment Variables for Java and Spark
6. Start the Spark Session
7. Load and setup Postgresql
8. Create a DataFrame with the rows from our dataset
9. Transform the data to fit the schema
    * get a row count for a comparison later
    * drop null rows
    * change the datatypes to integers and time stamps
    * data is ready to be added to new DFs 
10. Create DFs specifically for review_id, products, customers, vine_table
11. Configure the RDS instance using my super secret Amazon Endpoint ( the password and unique identifyer from the Amazon Endpoint has been removed for security purposes)
12. Send write commands to the database for tables, review_id_table, products, customers, and vine_table
13. Once that all works, do the same for **amazon_reviews_us_Video_v1_00.tsv.gz.**
    * Now here's a little aside.... I originally had digital video games selected as my 2nd dataset, however I was getting an error when I attempted to upload to the **products** table. <br/>
    ![Image](/Images/RDS%20write%20duplicate%20error%20with%20red%20box.jpg)<br/>

    Turns out that it kept finding the product ID in there already.  I changed the **mode** parameter to **"overwrite"** then **"ignore"** for running it any other time.
14. Then I ran some SQL queries to view the first 10 rows and to count all rows in pgAdmin to ensure the database updated.<br/><br/>
![Image](/Images/first%20pass%20-%20sql%20query%20for%20customers%20table.jpg) 
![Image](/Images/first%20pass%20-%20sql%20query%20for%20products%20table.jpg)<br/><br/>
![Image](/Images/first%20pass%20-%20sql%20query%20for%20review_id_table%20table.jpg) 
![Image](/Images/first%20pass%20-%20sql%20query%20for%20vine_table%20table.jpg)

<hr>

### Code Theory - Level 2

This time I went with only the Video Games Dataset: amazon_reviews_us_Video_Games_v1_00.tsv.gz
1. Copied all the steps used in Level 1 up to creating a DF for the dataset.
2. Selected only the wanted columns: Star rating, helpful votes, total votes, vine, and verified purchase
3. Dropped N/A entries and duplicates
4. Filtered to show products that had at least 10 votes and if the average helpful votes is over .5.
5. Filtered some more to break up the paid reviewers (vine peeps) vs. unpaid reviewers
6. Got some stats: count, mean, stddev, min and max
7. Calculated the Paid Reviewers
    * got the Vine reviewers who gave 5 stars
    * got all vine reviewers
    * calculated the % of paid reviewers that gave 5 stars
8. calculated the Unpaid Reviewers
    * got the unpaid reviewers who gave 5 stars
    * got all unpaid reviewers
    * calculated the % of unpaid reviewers that gave 5 stars

<hr>

## Vine Analysis

Are Vine reviews truly trustworthy when it comes to Video Games?

* Percent of 5 Star Ratings:
  * 45% of Vine Reviewers
  * 38% of Unpaid Reviewers
As almost half of Vine reviewers give 5 star reviews, I would be cautious to trust their opinions.

* The Average Total Votes show as:
  * 35 for Vine reviewers
  * 30 for Unpaid reviewers
The Average total votes are close in number which is great baseline for the rest of our comparison.

* The Standard Deviation for Helpful Votes shows as:
  *  30 for Paid Helpful Votes 
  *  77 for Unpaid Helpful Votes
For this metric, the Unpaid reviews have a wider SD so a bit more unreliable than the Vine reviews.
  
* The Average Star Rating:
  * 4.1 for Vine Reviewers
  * 3.3 for Unpaid Reviewers
While these numbers may look close together, we must remember this is a 5 star scale. Based on this alone, I'd be more likely to trust the Unpaid reviews.
  
With the information given below, I'd warn agains relying heavily on Vine reviews. 
