# ETL and EDA Project
I created this little project to put into practice some of the content and tools I've been studying, and the main objectives were: 

- Create a Python script to fetch datasets directly from Kaggle;
- Create a PostgreSQL database and update it with these datasets;
- Use dbt to manage and modify this database, creating the bronze, silver and gold layers, and populating them with different tables; 
- Carry out an Exploratory Data Analysis in search of business insights.

## 1. Business Problem
The company has a lot of data in different .csv files, and they would like to create a SQL database so that all the relevant professionals can access the data. 

In addition, in order to have all the information of an order, it is necessary to look at different files, and the company would like a single table to hold all this information.

## 2. Project Stages
1. Get the .csv files using a Python script;
2. Understand the relationship between these different files;
3. Create a PostgreSQL database hosted on the Render platform;
4. Insert the raw .csv files into the database, also using a Python script;
5. Create a dbt project to model the database, creating tables in the bronze, silver and gold layers;
6. Create the main tables in the gold layer bringing together all the relevant information from an order;
7. Carry out an EDA and provide insights for the company.

## 3. Conclusions
Using Python and SQL programming languages, and using dbt to manage/model the database, it was possible to make the .csv files accessible to everyone via a PostgreSQL database. 

Using SQL and also dbt, the data was treated in the different data layers, making it possible to create the main tables gathering the order information in the gold layer. 

By creating these tables with the data already cleaned and processed, it became much easier and faster to carry out EDA and provide insights to the business team.

## 4. References
Original dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce