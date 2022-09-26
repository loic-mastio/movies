#fileGenerator.py

'''General function for saving tasks's results / plots'''

def saveTaskResult(path: str, data, description: str):
    '''Save task result into a file where path contain filename + extension
    Format : {description}
             {data}
    '''
    with open(path, "w", encoding="utf8") as f:
        f.write(f"{description}")
        f.write(f"{data}")
        f.close()

def savePlot(path: str, plot):
    '''Save dataframe.plot into a file where path contain pathname + extension'''
    plot.get_figure().savefig(path)