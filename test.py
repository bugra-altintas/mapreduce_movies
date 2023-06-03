
#read csv file
import csv

def runtime_test():
#read csv file
    with open('movies.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        #skip header
        next(csv_reader)

        #header
        #['name', 'rating', 'genre', 'year', 'released', 'score', 'votes', 'director', 'writer', 'star', 'country', 'budget', 'gross', 'company', 'runtime']
        #The amount of time (in minutes) it would take to watch every movie in the dataset, back to back
        total_runtime = 0
        count = 0
        for line in csv_reader:
            try:
                total_runtime += float(line[14])
                count += 1
            except:
                pass
        print("Total runtime: ", total_runtime, " minutes")
        print("Average runtime: ", total_runtime/count, " minutes")

def employment_test():
    #read csv file
    with open('movies.csv', 'r') as csv_file:
        #How many times each actor has been top-billed (starred) in a movie
        csv_reader = csv.reader(csv_file)

        actors = {}
        #skip header
        next(csv_reader)

        #header
        #['name', 'rating', 'genre', 'year', 'released', 'score', 'votes', 'director', 'writer', 'star', 'country', 'budget', 'gross', 'company', 'runtime']
        for line in csv_reader:
            try:
                actor = line[9]
                if actor in actors:
                    actors[actor] += 1
                else:
                    actors[actor] = 1
            except:
                pass       

        #sort actors by actor names
        sorted_actors = sorted(actors.items(), key=lambda x: x[0])

        with open("employment.test","w+") as emp:
            for actor in sorted_actors:
                emp.write(actor[0] + "\t" + str(actor[1]) + "\n")

def ratescore_test():
    #read csv file
    with open('movies.csv', 'r') as csv_file:

        #Average number of IMDb votes on G, PG, PG-13 and R rated movies
        csv_reader = csv.reader(csv_file)

        #skip header
        next(csv_reader)

        #header
        #['name', 'rating', 'genre', 'year', 'released', 'score', 'votes', 'director', 'writer', 'star', 'country', 'budget', 'gross', 'company', 'runtime']
        g_votes = 0
        g_count = 0
        pg_votes = 0
        pg_count = 0
        pg13_votes = 0
        pg13_count = 0
        r_votes = 0
        r_count = 0
        for line in csv_reader:
            try:
                rating = line[1]
                votes = float(line[6])
                if rating == "G":
                    g_votes += votes
                    g_count += 1
                elif rating == "PG":
                    pg_votes += votes
                    pg_count += 1
                elif rating == "PG-13":
                    pg13_votes += votes
                    pg13_count += 1
                elif rating == "R":
                    r_votes += votes
                    r_count += 1
            except:
                pass

        with open("ratescore.test","w+") as ratescore:
            ratescore.write("G\t" + str(g_votes/g_count) + "\n")
            ratescore.write("PG\t" + str(pg_votes/pg_count) + "\n")
            ratescore.write("PG-13\t" + str(pg13_votes/pg13_count) + "\n")
            ratescore.write("R\t" + str(r_votes/r_count) + "\n")

def genrescore_test():
    #read csv file
    with open('movies.csv', 'r') as csv_file:

        #The average IMDb score of genres that have more than 9 movies
        csv_reader = csv.reader(csv_file)

        #skip header
        next(csv_reader)

        #header
        #['name', 'rating', 'genre', 'year', 'released', 'score', 'votes', 'director', 'writer', 'star', 'country', 'budget', 'gross', 'company', 'runtime']

        genres = {}
        for line in csv_reader:
            try:
                genre = line[2]
                score = float(line[5])
                if genre in genres:
                    genres[genre].append(score)
                else:
                    genres[genre] = [score]
            except:
                pass

        #sort genres by genre names
        sorted_genres = sorted(genres.items(), key=lambda x: x[0])

        with open("genrescore.test","w+") as genrescore:
            for genre in sorted_genres:
                if len(genre[1]) > 9:
                    genrescore.write(genre[0] + "\t" + str(sum(genre[1])/len(genre[1])) + "\n")




runtime_test()
employment_test()
ratescore_test()
genrescore_test()









