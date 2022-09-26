#timer.py
import time

def time_convert(sec: int) ->str:
  '''Convert seconds to hh:MM:SS'''
  mins = sec // 60
  sec = sec % 60
  hours = mins // 60
  mins = mins % 60
  return "Time Lapsed = {0}:{1}:{2}".format(int(hours),int(mins),sec)

