# dynamo-mysql-to-mysql
This is my first project developed with the help of Gemini 2.0 flash (currently experimental) using prompting.

I tried cursor but I found more easier to prompt engineering and use a step-by-step approach.

I am not currently a developer or a Python expert but I created something that works, sorry for any bad coding practice you might find here.

You can skip sources of data as required on the script.py file, e.g use DynamoDB or Mysql only.

You can use any Mysql destination database.

If you run the script and gives type errors when running the data insertion for tables whose origin is DynamoDB, you can add exceptions in **utils\local_mysql_utils.py->generate_ddl**
