The driver class "Hw3" assumes that there is a 'movies.tsv' file in the hadoop file system.
So, before running the application, "python3 csv2tsv.py <movies.csv path>" command should be executed.
It creates the .tsv file at the same place with .csv file.
Then, "hadoop fs -put <movies.tsv path>" command should be executed.
The application is ready to start.