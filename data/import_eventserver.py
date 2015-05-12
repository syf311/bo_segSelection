"""
Import sample data for E-Commerce Recommendation Engine Template
"""

import predictionio
import argparse
import random

SEED = 3

def import_events(client, file):
  random.seed(SEED)
  count = 0
  print client.get_status()
  print "Importing data..."

  # generate 10 users, with user ids u1,u2,....,u5
  user_ids = ["u%s" % i for i in range(1, 6)]
  for user_id in user_ids:
    print "Set user", user_id
    client.create_event(
      event="$set",
      entity_type="user",
      entity_id=user_id
    )
    count += 1

  # generate 5 segs, with item ids i1,i2,....,i5
  
  seg_ids = ["s%s" % i for i in range(1, 6)]
  for seg_id in seg_ids:
    print "Set seg", seg_id
    client.create_event(
      event="$set",
      entity_type="seg",
      entity_id=seg_id
    )
    count += 1

  # genreate connect events
  f = open(file, 'r')
  print "Importing connect events..."
  for line in f:
    data = line.rstrip('\r\n')
    conn = data.split(" ")
    client.create_event(
      event="connect",
      entity_type="user",
      entity_id=conn[0], # use the count num as user ID
      target_entity_type="seg",
      target_entity_id=conn[1]
    )
    count += 1
  f.close()

  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for seg selection recommendation engine")
  parser.add_argument('--access_key', default='hIYvrzPPI9bHlFvw2dJDVEkeWyJo9ZeP4TOy9dxccHJahKhi7dUpKJ0Wz8V00cgb')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/connect.txt")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
