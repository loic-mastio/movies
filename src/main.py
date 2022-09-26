
from data import *
from tasksMaking import *


class Init():
    '''Classe permettant l'initialisation des autres classes, nous appellerons les méthodes
     des autres classes depuis celle-ci'''
    def __init__(self):
        self.bdd = Data()
        self.tasks = None
    def launchStorageAndClean(self):
        init.bdd.initializeConnexion()
        self.bdd.storeData()
        self.bdd.cleanData()
    def launchTasks(self):
        init.tasks = TaskMaker(init.bdd)
        init.tasks.Task()
        init.bdd.close()

if __name__ == '__main__':
    init = Init()
    init.launchStorageAndClean()
    init.launchTasks()
