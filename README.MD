# Analyzing Timelines and Effectiveness in US Healthcare Delivery

This Project Dives into real world hospital performance, mainly focusing on emergency services using CMS dataset to wait times across United States.

# Tools Used:
 Data Extraction and Data Cleaning - Python, Pandas
 Database- AWS Aurora and RDS, Postgres
 Dashboard Creation & visualization - Tableau

# Key purpose
This project uses a combination of python and tableau dashboard to explore the insights of emergency weight times, stroke care, CT wait time, sepsis care. Using OP_18b, OP_18c, ED_2_strata_1, ED_2_strata_2, STK_05, SEP_1

# Key Analysis:
1. Wait Times By State
2. Top / Bottom 10 Hospitals for ED visits
3. Changes in ED Throughput over the year
4. Time Before they leave
5. Variation in ED Wait Scores Across EDV levels
6. Count of Overall Records Used
7. Count of No of samples used


# Files
1. CMS_Crawler.py - API Integration with postgres database, along with normalization of the table
2. Data_Cleaning.py - Used to clean data by remove unused rows, leading zeros, extracting the feature to create new, Date format changes,scaling.
3. Env.py- Hidden file which is not in my repository, but used to store postgres Database credentials.

# Live Interactive Dashboard
Dashboard-1 : https://harshith444.github.io/Medical-Emergency-Wait-Time-Analysis/




