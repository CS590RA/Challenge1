#!/usr/bin/env python

from operator import itemgetter

import sys

current_key = None 
current_rate = 0
key = None

for line in sys.stdin:
  line = line.strip()

  key, rate = line.split(' ', 1)
  #print key, "-", rate
  try:
    rate = float(rate)
  except ValueError as e:
    print e

  if current_key == key:
    current_rate += rate
  else:
    print "%s %s" %(current_key, current_rate)
    current_rate = rate
    current_key = key

if current_key == key:
  print "%s %s" %(current_key, current_rate)

