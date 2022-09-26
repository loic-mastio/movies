from utils.fileGenerator import *
import pandas as pd
from utils.applyFunctions import *
from utils.timer import *

class TaskMaker():
    def __init__(self, dataClass):
        self.__moviesDF = dataClass.getDF()["movies"]["transformedDS"]
        self.__ratingsDF = dataClass.getDF()["ratings"]["transformedDS"]
        self.__cursorDB = dataClass.getCursor()

    #1.1
    def nbrMovies(self):
        nbrMovies = len(self.__moviesDF["title"].index)
        saveTaskResult("../output/firstTasks/1-1.txt", nbrMovies, "Nombre de films figurant dans la base de données :\n")
    #1.2
    def nbrUsers(self):
        nbrUsers = len(self.__ratingsDF["user_id"].unique())
        saveTaskResult("../output/firstTasks/1-2.txt", nbrUsers, "Nombre d'users figurant dans la base de données :\n")

    #1.3 & 1.4 Tasks
    def distributionRating(self):
        distribution = pd.DataFrame(self.__ratingsDF['rating'].unique(), columns=["rating"])
        #Count all values for each titles
        distribution["occurency"] = distribution["rating"].map(self.__ratingsDF['rating'].value_counts())
        #Count but return percentage when multiply by 100 the value
        distribution["percentage"] = distribution["rating"].map(self.__ratingsDF['rating'].value_counts(normalize=True) * 100)
        saveTaskResult("../output/firstTasks/1-3-4.txt", distribution, "Distribution de notes:\n")
        savePlot("../output/firstTasks/plotOccurency.png", distribution.plot(x="rating", y="occurency", kind="bar"))
        savePlot("../output/firstTasks/plotPercentage.png", distribution.plot(x="rating", y="percentage", kind="bar"))
    # 2.1
    def createColumnLiked(self):
        '''Create a new column in ratings DF by applying function'''
        self.__ratingsDF["liked"] = self.__ratingsDF["rating"].apply(columnLiked)
        saveTaskResult("../output/secondTasks/2-1.txt", self.__ratingsDF, "Column liked added:\n")
    #2.2
    def mostLikedGenre(self):
        #getMean liked value by unique movie_id
        meanLiked = self.__ratingsDF[["movie_id", "liked"]].groupby("movie_id").mean().reset_index()
        #merge with movie DF
        self.__moviesDF = pd.merge(self.__moviesDF, meanLiked, on=["movie_id"])
        #group DF by genre and get liked mean value, sort by Desc and get only 10 values
        meanRatingGenre = self.__moviesDF[["genre", "liked"]].groupby(["genre"]).mean().sort_values(by=["liked"], ascending=False).head(10)
        saveTaskResult("../output/secondTasks/2-2.txt", meanRatingGenre, "Top 10 genre liked:\n")
    #3.1
    def topMovieLiked(self):
        topMovieLiked = self.__moviesDF[self.__moviesDF["occurency"] > 4].sort_values(by=["liked", "occurency"],
                                                                                      ascending=False).head(10)
        saveTaskResult("../output/thirdTasks/3-1.txt", topMovieLiked[["title", "occurency", "liked"]].reset_index(),
                       "Top 10 movie liked with minimum 5 ratings:\n")
    #3.2
    def mostEvaluatedMovieInSpecificYear(self, year):
        #Change column to datetime type
        self.__ratingsDF["rating_timestamp"] = pd.to_datetime(self.__ratingsDF["rating_timestamp"], unit='s')
        #Get only movie_id from movie rated in 2020
        specialYear = self.__ratingsDF["movie_id"][self.__ratingsDF["rating_timestamp"].dt.year == year]
        #Get the most liked movie in 2020
        topRatedMovieID = specialYear.value_counts().index[0]
        #Filter on movie id
        topRatedMovieTitle = self.__moviesDF["title"][self.__moviesDF["movie_id"] == topRatedMovieID]
        saveTaskResult("../output/thirdTasks/3-2.txt", topRatedMovieTitle.reset_index(), "Top movie liked in {}:\n".format(year))
    #4.1
    def executequery(self, string):
        self.__cursorDB.execute(string)
    def createIndexAndSeePerformance(self):
        start = time.time()
        self.executequery("select rating from ratings where user_id = 255")
        self.executequery("select SUM(rating) from ratings group by user_id ")
        end = time.time()
        beforeIndex = "Avant Index: {}\n".format(time_convert(end - start))
        self.executequery("CREATE INDEX IF NOT EXISTS ratings_userId_ix ON ratings (user_id);")
        start = time.time()
        self.executequery("select rating from ratings where user_id = 255")
        self.executequery("select SUM(rating) from ratings where user_id = 255 group by user_id ")
        end = time.time()
        afterIndex = "Après Index: " + time_convert(end - start)
        self.executequery("DROP INDEX ratings_userId_ix")
        saveTaskResult("../output/fourthTask/4-1.txt", (beforeIndex + afterIndex), "Résultats avant / après index : \n")

    def Task(self):
        self.nbrMovies()
        self.nbrUsers()
        self.distributionRating()
        self.createColumnLiked()
        self.mostLikedGenre()
        self.topMovieLiked()
        self.mostEvaluatedMovieInSpecificYear(2020)
        self.createIndexAndSeePerformance()