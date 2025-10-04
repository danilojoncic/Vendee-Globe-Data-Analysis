#The purpose of this file is to run the same transforamtions editing and saving as those done in
#the parser.py file. Except this time instead of using pandas dataframes i used PySpark Dataframes,
#possibly rdd or Spark SQL directly

#For the purpose of making the project easier on the resources, the spark part of it (i.e. this) will be using its
#own docker compose, as putting it with all the others in the main compose file and running it would probbably
#make my macbook air cry