import sqlite3

import pandas as pd
from utils.timer import *
from utils.applyFunctions import *
from utils.fileGenerator import *
pd.options.mode.chained_assignment = None

class Data():
    def __init__(self):
        self.__connectorDB = None
        self.__cursorDB = None
        self.__pathDB = "../data/movies.sqlite"
        self.__idMoviesDuplicated = []
        self.__dataDF = {"movies": {}, "ratings": {}}

    '''Getter'''
    def getCursor(self):
        return self.__cursorDB
    def getDF(self):
        return self.__dataDF

    def initializeConnexion(self):
        '''Initialize classes attributs : connector and cursor'''
        self.connectDatabase()
        self.__cursorDB = self.__connectorDB.cursor()

    def connectDatabase(self):
        """ create a database connection to the SQLite database
                specified by the db_file
            :param pathDB: database file
            :return: Connection object
            Beware, if your path is wrong you'll have a connection object from the value return too
            """
        try:
            self.__connectorDB = sqlite3.connect(self.__pathDB)
        except:
            raise

    def storeData(self):
        '''Execute SQL query and store result into dict containing DF'''
        self.__dataDF["movies"]["initialDS"] = pd.read_sql('select * from movies', self.__connectorDB)
        self.__dataDF["movies"]["transformedDS"] = self.__dataDF["movies"]["initialDS"]
        self.__dataDF["ratings"]["initialDS"] = pd.read_sql('select * from ratings', self.__connectorDB)
        self.__dataDF["ratings"]["transformedDS"] = self.__dataDF["ratings"]["initialDS"]


    def handleDuplicated(self):
        '''Main function for cleaning duplicated by counting occurency of all movie_id, and changing all movie_id into
        ratings table who are the movies duplicated
        '''
        self.catchDuplicate()
        self.countOccurency()
        duplicatedDF = self.__dataDF["movies"]["transformedDS"][["movie_id", "title", "genre", "occurency"]][
            self.__dataDF["movies"]["transformedDS"]["isDuplicated"] == True]
        duplicatedDF.update(self.changemovieID(duplicatedDF))
        self.__dataDF["movies"]["transformedDS"].update(duplicatedDF)

    def cleanGenre(self):
        '''Call general functions in applyFunctions.py'''
        self.__dataDF["movies"]["transformedDS"]["genre"] = self.__dataDF["movies"]["transformedDS"]["genre"].apply(
            cleanDuplicateGenre)
        self.__dataDF["movies"]["transformedDS"]["genre"] = self.__dataDF["movies"]["transformedDS"]["genre"].apply(
            sortGenre)


    def cleanData(self):
        '''Main function for cleaning the data, call others functions'''
        self.handleDuplicated()
        self.cleanGenre()
        #Keep only unique title
        self.__dataDF["movies"]["transformedDS"] = self.__dataDF["movies"]["transformedDS"][self.__dataDF["movies"]
                                                                                            ["transformedDS"]["movie_id"] > 0]

        #Clean DF for modify type and drop useless columns
        del self.__dataDF["movies"]["transformedDS"]["isDuplicated"]
        self.__dataDF["movies"]["transformedDS"]["movie_id"] = self.__dataDF["movies"]["transformedDS"]["movie_id"].astype("int")

    def countOccurency(self):
        '''Add occurency to all movies, create new column called occurency who count how many movie_id are
        present in ratings's table'''
        self.__dataDF["movies"]["transformedDS"]["occurency"] = self.__dataDF["movies"]["transformedDS"]["movie_id"].map\
            (self.__dataDF["ratings"]["transformedDS"]["movie_id"].value_counts())

    def catchDuplicate(self):
        #Create a new column called isDuplicated and set True or False if multiple title are present
        self.__dataDF["movies"]["transformedDS"]["isDuplicated"] = self.__dataDF["movies"]["transformedDS"].duplicated(subset="title", keep=False)
    def changemovieID(self, duplicatedDF):
        #get unique title for each duplicated
        uniqueTitle = duplicatedDF["title"].unique()
        for title in uniqueTitle:
            inc = 0
            # temporary DF for sorting by tittle
            tempDF = duplicatedDF[["title", "occurency", "movie_id", "genre"]][duplicatedDF["title"] == title]
            tempDF["maxOccurency"] = tempDF["occurency"].max()
            #Get DF with max occurency
            movieIdMaxOccurency = tempDF[["movie_id", "genre", "title"]][tempDF["occurency"] == tempDF["maxOccurency"]]
            #If there is multiples max occurency
            if len(movieIdMaxOccurency) > 1:
                #update DF with movie_id set to -1 to all duplicated max occurency except one
                movieIdMaxOccurency.update(self.cleanDoubleMaxOccurency(movieIdMaxOccurency))
                tempDF.update(movieIdMaxOccurency)
            while inc < len(tempDF):
                #If the duplicate is not the one with max occurency
                if tempDF["movie_id"].iloc[inc] != movieIdMaxOccurency["movie_id"].max() and tempDF["movie_id"].iloc[inc] != -1:
                    #Store movie_id
                    movieIdSoonDeleted = tempDF["movie_id"].iloc[inc]
                    #Append genre from other duplicate to biggest occurency in tempDF
                    tempDF["genre"][tempDF["movie_id"] == movieIdMaxOccurency["movie_id"].max()] = "|".join\
                        ([tempDF["genre"][tempDF["movie_id"] == movieIdMaxOccurency["movie_id"].max()].item(), tempDF["genre"]
                         .iloc[inc]])
                    #Set movie_id to -1 when the duplicate need to be deleted
                    tempDF["movie_id"].iloc[inc] = -1
                    #Change in ratings table the movie_id from the duplicate who is gonna be deleted by the movie_id from the highest occurency
                    self.__dataDF["ratings"]["transformedDS"]["movie_id"][self.__dataDF["ratings"]["transformedDS"]["movie_id"]
                                                                          == movieIdSoonDeleted] = movieIdMaxOccurency["movie_id"].max()
                inc += 1
            duplicatedDF.update(tempDF)
        return duplicatedDF

    '''Clean DS who has the same occurency, title and is the maxOccurency'''
    def cleanDoubleMaxOccurency(self, movieIdMaxOccurency):
            #Set increment to keep the 0 index value and transform others
            inc = 1
            while inc < len(movieIdMaxOccurency):
                movieIdSoonDeleted = movieIdMaxOccurency.iloc[inc]["movie_id"]
                movieIdMaxOccurency["genre"] = movieIdMaxOccurency["genre"].values.astype(str)
                #Append Gender to the max occurency value not modified
                movieIdMaxOccurency["genre"].iloc[0] = "|".join(filter(None, [movieIdMaxOccurency.iloc[0]["genre"],
                                                                              movieIdMaxOccurency.iloc[inc]["genre"]]))
                self.__dataDF["ratings"]["transformedDS"]["movie_id"][self.__dataDF["ratings"]["transformedDS"]["movie_id"]
                                                                      == movieIdSoonDeleted] = movieIdMaxOccurency.iloc[0]["movie_id"]
                #Set futur deleted row movie_id to -1
                movieIdMaxOccurency["movie_id"].iloc[inc] = -1
                inc += 1
            return movieIdMaxOccurency

    def close(self):
        self.__connectorDB.close()