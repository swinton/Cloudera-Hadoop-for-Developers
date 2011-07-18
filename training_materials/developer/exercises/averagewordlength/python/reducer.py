#!/usr/bin/python

import sys

wordcount = 0.0
lettercount = 0

key = None
for input in sys.stdin.readlines():
  input = input.rstrip()
  parts = input.split("\t")

  if len(parts) < 2:
    continue

  newkey=parts[0]
  wordlen=int(parts[1])

  if not key: 
    key = newkey

  if key != newkey:
    print key + "\t" + str(lettercount / wordcount)
    key = newkey;
    wordcount = 0.0
    lettercount = 0

  wordcount = wordcount + 1.0
  lettercount = lettercount + wordlen

if key != None:
  print key + "\t" + str(lettercount / wordcount)
