# Exercices - Data Management

Ces exercices tournent autour de la manipulation de données dans le but d'évaluer le ou la candidat·e sur ses capacités d'organisation et de réflexion pour répondre à un problème.

Pour ces exercices, nous proposons de travailler sur les données fournies par nos soins qui présentent des enjeux proches de ceux rencontrés en travaillant sur les données du SNDS.
Il s'agit d'évaluation de films par des utilisateurs de la plateforme [IMDB](https://github.com/sidooms/MovieTweetings).

## Préparation de l'environnement et attentes

Nous utiliserons Python en version [3.10](https://www.python.org/downloads/) comme outil de script.
Il est attendu du ou de la candidat·e les fichiers finaux permettant d'obtenir les résultats décrits ci-après.

La base de données est fournie par le fichier SQLite `data/movies.sqlite`.
Deux tables nous intéressent ici : 

 - `movies` : comporte les colonnes `movie_id`, `title` et `genre`
 - `ratings`: comporte les colonnes `user_id`, `movie_id`, `rating` et `rating_timestamp`

L'exploitation de cette base de données peut-être réalisée en utilisant le module `sqlite3` de la bibliothèque standard :

```python
import sqlite3

with sqlite3.connect("data/movies.sqlite") as co:
    co.execute(
            "SELECT rating FROM ratings LIMIT 10"
        ).fetchall()
```

Néanmoins, nous encourageons le ou la candidat·e à utiliser les outils de son choix (exemples : `pandas`, `sqlalchemy`, `jupyter`), tant que le travail est documenté pour la reproductibilité (définition d'un `requirements.txt` à envoyer avec le code source et les résultats).

Par ailleurs, nous attendons que les résultats soient inclus dans un fichier de rapport généré par le code source.
La mise en forme peut être très simple :

```python
nb_films = 10
with open("results.txt", "a", encoding="utf8") as f:
    f.write(f"{nb_films} films figurent dans la base de données.")
```

### Évaluation

L'enjeu de ces exercices est de mettre en valeur votre façon de raisonnner et de vous organiser.
Nous serons particulièrement intéressés par les structures de programmation que vous mettrez en place.
Nous accordons, au sein de notre équipe, une grande importance au fait de pouvoir reprendre le travail d'un collaborateur.
La reproductibilité et la facilité de prise en main de votre travail sera également un critère d'évaluation important.  
En revanche, votre connaissance fine de la grammaire Python, si elle est intéressante, ne sera pas un critère déterminant de ces exercices.

Ainsi, les fichiers sources en retour seront évalués selon les modalités suivantes :

- **Organisation** : comment le problème a-t-il été abordé ?
- **Clean code** : le code source est-il de qualité, bien construit et organisé ? Le travail est-il reproductible et maintenable ?
- **Résultats** : les résultats sont-ils conformes aux attentes ?

## Tâches

### 1. Dénombrements

- 1.1 Combien de films figurent dans la base de données ?
- 1.2 Combien d'utilisateurs différents figurent dans la base de données ?
- 1.3 Quelle est la distribution des notes renseignées ?  
    **Bonus** : créer un histogramme.
- 1.4 Nous souhaitons finalement obtenir une table des fréquences pour exprimer en pourcentage la répartition des notes.

### 2. Sélection et enrichissement des données

- 2.1 Afin de mettre en place un certain modèle statistique, nous devons transformer la note `rating` en deux modalités : l'utilisateur a-t-il aimé ou pas le film ?
    Créer une nouvelle colonne `liked` dans la table `ratings` avec les valeurs suivantes : `0` pour les notes [0-6] et `1` pour les notes [7-10].
- 2.2 Quels sont les genres les mieux notés par les utilisateurs ? Nous souhaitons obtenir le **top 10** des combinaisons de genres de films aimés par les utilisateurs (à l'aide de la nouvelle colonne `liked`).

### Sélections avancées

- 3.1 Quels sont les titres des films les plus aimés des internautes ?  
    Nous cherchons les **10** films les mieux notés en moyenne par les utilisateurs, avec un minimum de **5** notations pour que la mesure soit pertinente.
- 3.2 Quel est le film le plus noté durant l'année 2020 ?  
    **Note** : la colonne `rating_timestamp` est fournie dans la base sous forme d'[heure Unix](https://fr.wikipedia.org/wiki/Heure_Unix).

### Gestion des données

- 4.1 Afin de retrouver plus rapidement les notes d'un utilisateur en particulier, nous souhaitons mettre en place un index sur les id utilisateurs.
    Constatez-vous une différence de performances en recherchant les évaluations données par l'internaute `255` ?

## Procédure

### 1. Structure du code

Le code a été organisé en 3 différentes classes disposant de méthodes qui leur sont propres :
```python
class Init():
    def __init__(self)
    def launchStorageAndClean(self)
    def launchTasks(self)
```

```python
class Data():
    def __init__(self)
    def getCursor(self)
    def getDF(self)
    def initializeConnexion(self)
    def connectDatabase(self)
    def storeData(self)
    def handleDuplicated(self)
    def cleanGenre(self)
    def cleanData(self)
    def countOccurency(self)
    def catchDuplicate(self, table, category, arrayColumn)
    def changemovieID(self, duplicatedDF)
    def cleanDoubleMaxOccurency(self, title, movieIdMaxOccurency)
    def close(self)
```
```python
class TaskMaker():
    def __init__(self, dataClass)
    def nbrMovies(self)
    def nbrUsers(self)
    def distributionRating(self)
    def createColumnLiked(self)
    def mostLikedGenre(self)
    def topMovieLiked(self)
    def mostEvaluatedMovieInSpecificYear(self, year)
    def executequery(self, string)
    def createIndexAndSeePerformance(self)
    def Task(self)
```
En complément, des fonctions plus générales sont utilisées, elles se trouvent dans le dossier "utils"
```python
#applyFunctions.py

def sortGenre(string: str) -> str
def cleanDuplicateGenre(string: str) -> str
def columnLiked(note: int) -> int
```
```python
#fileGenerator.py

def saveTaskResult(path: str, data, description: str)
def savePlot(path: str, plot)
```
```python
#timer.py


def time_convert(sec: int) ->str
```
### 2. Selection et stockage des données

Les données ont été sélectionnées grâce à des requêtes SQL très basiques.
L'optimisation de ces requêtes n'a pas été effectuée, de simples 'select *' ont été effectués

Les données ont été stockées dans des dictionnaires contenant d'autres dictionnaires qui contiennent des DF.
```python
self.__dataDF["movies"]["initialDS"] = pd.read_sql('select * from movies', self.__connectorDB)
self.__dataDF["ratings"]["initialDS"] = pd.read_sql('select * from ratings', self.__connectorDB)
```
Puis ces données ont été dupliquées dans d'autres dictionnaires afin de manipuler ces dernières
```python
self.__dataDF["movies"]["transformedDS"] = self.__dataDF["movies"]["initialDS"]
self.__dataDF["ratings"]["transformedDS"] = self.__dataDF["ratings"]["initialDS"]
```

### 3. Nettoyage des données

Les données ont subis différents traitements afin de nettoyer le jeu de données.

/!\ La table movies contenait plusieurs films ayant le même titre, le choix a été fait de traiter ces derniers comme un
unique film et par conséquent des duplications sont présentes dans la BDD

Toutes les main fonctions de nettoyage sont appellées dans
```python
def cleanData(self)
```
#### 1. Nettoyage des duplicates
Toutes les main fonctions de nettoyage des duplicates sont appellées dans
```python
def handleDuplicated(self)
```

##### 1. Flag les duplicates par le titre
```python
#Create a new column called isDuplicated and set True or False if multiple title are present 
def catchDuplicate(self):
    #Create a new column called isDuplicated and set True or False if multiple title are present
    self.__dataDF["movies"]["transformedDS"]["isDuplicated"] = self.__dataDF["movies"]["transformedDS"].duplicated(subset="title", keep=False)
                                                                                ["title"])
```
##### 2. Flag les occurency de chaques films
```python
def countOccurency(self):
    '''Add occurency to all movies, create new column called occurency who count how many movie_id are
    present in ratings's table'''
    self.__dataDF["movies"]["transformedDS"]["occurency"] = self.__dataDF["movies"]["transformedDS"]["movie_id"].map\
            (self.__dataDF["ratings"]["transformedDS"]["movie_id"].value_counts())
```
##### 3. Dupliquer DF contenant uniquement les doublons
```python
    duplicatedDF = self.__dataDF["movies"]["transformedDS"][["movie_id", "title", "genre", "occurency"]][
        self.__dataDF["movies"]["transformedDS"]["isDuplicated"] == True]
```
##### 4. Clean des duplicates

Afin d'avoir un unique film par titre dans la table movie & rating, les doublons avec le moins
d'occurences dans la table ratings sont supprimés.

Afin de conserver leur genre, ils sont concatener au film ayant le plus d'occurence.

```python
duplicatedDF.update(self.changemovieID(duplicatedDF))
```
```python
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
            movieIdMaxOccurency.update(self.cleanDoubleMaxOccurency(title, movieIdMaxOccurency))
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
```
```python
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
```
##### 5. Garder uniquement les films originaux
```python
self.__dataDF["movies"]["transformedDS"] = self.__dataDF["movies"]["transformedDS"][self.__dataDF["movies"]
                                                                                            ["transformedDS"]["movie_id"] > 0]
```

##### 6. Tasks

###### 1.1 NbrMovies

Après avoir clean le DS, les films sont uniques donc:
```python
nbrMovies = len(self.__moviesDF["title"].index)
```

###### 1.2 NbrUsers
```python
nbrUsers = len(self.__ratingsDF["user_id"].unique())
```

###### 1.3 & 1.4 Distribution
```python
def distributionRating(self):
    distribution = pd.DataFrame(self.__ratingsDF['rating'].unique(), columns=["rating"])
    #Count all values for each titles
    distribution["occurency"] = distribution["rating"].map(self.__ratingsDF['rating'].value_counts())
    distribution["percentage"] = distribution["rating"].map(self.__ratingsDF['rating'].value_counts(normalize=True) * 100)
    #Percentage
    saveTaskResult("../output/firstTasks/1-3-4.txt", distribution, "Distribution de notes:\n")
    savePlot("../output/firstTasks/plotOccurency.png", distribution.plot(x="rating", y="occurency", kind="bar"))
    savePlot("../output/firstTasks/plotPercentage.png", distribution.plot(x="rating", y="percentage", kind="bar"))

```

###### 2.1 Create column liked
```python
def createColumnLiked(self):
    '''Create a new column in ratings DF by applying function'''
    self.__ratingsDF["liked"] = self.__ratingsDF["rating"].apply(columnLiked)
    saveTaskResult("../output/secondTasks/2-1.txt", self.__ratingsDF, "Column liked added:\n")
```

###### 2.2 Most liked genre
```python
def mostLikedGenre(self):
    #getMean liked value by unique movie_id
    meanLiked = self.__ratingsDF[["movie_id", "liked"]].groupby("movie_id").mean().reset_index()
    #merge with movie DF
    self.__moviesDF = pd.merge(self.__moviesDF, meanLiked, on=["movie_id"])
    #group DF by genre and get liked mean value, sort by Desc and get only 10 values
    meanRatingGenre = self.__moviesDF[["genre", "liked"]].groupby(["genre"]).mean().sort_values(by=["liked"], ascending=False).head(10)
    saveTaskResult("../output/secondTasks/2-2.txt", meanRatingGenre, "Top 10 genre liked:\n")
 
```

###### 3.1 Top Movie Liked
```python
def topMovieLiked(self):
    topMovieLiked = self.__moviesDF[self.__moviesDF["occurency"] > 4].sort_values(by=["liked", "occurency"],
                                                                                      ascending=False).head(10)
    saveTaskResult("../output/thirdTasks/3-1.txt", topMovieLiked[["title", "occurency", "liked"]].reset_index(),
                       "Top 10 movie liked with minimum 5 ratings:\n")
```

###### 3.2 Most Evaluated Movie in 2020
```python
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
```

###### 4.1 Index monitoring
```python
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
```