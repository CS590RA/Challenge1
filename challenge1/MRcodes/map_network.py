#!/usr/bin/env python

import sys

for line in sys.stdin:
  line = line.strip()
  tmp = line.split()
  srcIP = tmp[8]
  dstIP = tmp[13]
  rate = tmp[18]
  key = srcIP + '-' + dstIP
  #for word in words:
  #  print '%s %s' % (word, 1)

  print '%s %s' % (key, rate)
