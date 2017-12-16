#!/usr/bin/python3

import praw
import os
import logging.handlers
import time
import sys
import configparser
import signal
import requests
import traceback
import json
import sqlite3
from datetime import datetime
from datetime import timezone
from datetime import timedelta

### Config ###
LOG_FOLDER_NAME = "logs"
SUBREDDIT = "ncaaBBallStreams"
USER_AGENT = "NCAABBall (by /u/Watchful1)"
LOOP_TIME = 15*60
DATABASE_NAME = "database.db"

estTimezone = timezone(timedelta(hours=-5))

### Logging setup ###
LOG_LEVEL = logging.DEBUG
if not os.path.exists(LOG_FOLDER_NAME):
    os.makedirs(LOG_FOLDER_NAME)
LOG_FILENAME = LOG_FOLDER_NAME+"/"+"bot.log"
LOG_FILE_BACKUPCOUNT = 5
LOG_FILE_MAXSIZE = 1024 * 256

log = logging.getLogger("bot")
log.setLevel(LOG_LEVEL)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_stderrHandler = logging.StreamHandler()
log_stderrHandler.setFormatter(log_formatter)
log.addHandler(log_stderrHandler)
if LOG_FILENAME is not None:
	log_fileHandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=LOG_FILE_MAXSIZE, backupCount=LOG_FILE_BACKUPCOUNT)
	log_formatter_file = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
	log_fileHandler.setFormatter(log_formatter_file)
	log.addHandler(log_fileHandler)


dbConn = sqlite3.connect(DATABASE_NAME)
c = dbConn.cursor()
c.execute('''
	CREATE TABLE IF NOT EXISTS threads (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		GameID VARCHAR(80) NOT NULL,
		ThreadID VARCHAR(80) NOT NULL,
		CreationDate TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		Deleted BOOLEAN DEFAULT 0,
		UNIQUE (GameID, ThreadID)
	)
''')
dbConn.commit()


def getGameByID(gameID):
	c = dbConn.cursor()
	result = c.execute('''
		SELECT ThreadID, CreationDate
		FROM threads
		WHERE GameID = ?
	''', (gameID,))

	resultTuple = result.fetchone()

	if not resultTuple:
		return None
	else:
		return {'threadid': resultTuple[0], 'creationdate': resultTuple[1]}


def postGame(gameID, threadID):
	c = dbConn.cursor()
	try:
		c.execute('''
			INSERT INTO threads
			(GameID, ThreadID)
			VALUES (?, ?)
		''', (gameID, threadID))
	except sqlite3.IntegrityError:
		return False

	dbConn.commit()
	return True


def getGames():
	c = dbConn.cursor()
	result = c.execute('''
		SELECT ThreadID, GameID, CreationDate
		FROM threads
		WHERE Deleted = 0
	''')

	out = []
	for game in result.fetchall():
		out.append({'threadid': game[0], 'gameid': game[1], 'date': game[2]})

	return out


def markGameDeleted(gameID):
	c = dbConn.cursor()
	c.execute('''
		UPDATE threads
		set Deleted = 1
		where GameID = ?
	''', (gameID,))
	dbConn.commit()


def signal_handler(signal, frame):
	log.info("Handling interrupt")
	dbConn.commit()
	dbConn.close()
	sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

log.debug("Connecting to reddit")

once = False
debug = False
user = None
if len(sys.argv) >= 2:
	user = sys.argv[1]
	for arg in sys.argv:
		if arg == 'once':
			once = True
		elif arg == 'debug':
			debug = True
else:
	log.error("No user specified, aborting")
	sys.exit(0)


try:
	r = praw.Reddit(
		user
		,user_agent=USER_AGENT)
except configparser.NoSectionError:
	log.error("User "+user+" not in praw.ini, aborting")
	sys.exit(0)

log.info("Logged into reddit as /u/"+str(r.user.me()))

while True:
	startTime = time.perf_counter()
	log.debug("Starting run")

	currentDate = datetime.utcnow().replace(tzinfo=timezone.utc)
	timeslug = currentDate.astimezone(estTimezone).strftime("%Y/%m/%d")
	url = "http://data.ncaa.com/jsonp/scoreboard/basketball-men/d1/"+timeslug+"/scoreboard.html"
	wrappedJson = requests.get(url=url, headers={'User-Agent': USER_AGENT}).text
	actualJson = wrappedJson.replace("callbackWrapper(", "").strip(");")
	jsonData = json.loads(actualJson)

	sub = r.subreddit(SUBREDDIT)
	finalGames = set()
	for game in jsonData['scoreboard'][0]['games']:
		gamePostDatetime = datetime.utcfromtimestamp(int(game['startTimeEpoch'])).replace(tzinfo=timezone.utc) - timedelta(hours=1)
		if gamePostDatetime < currentDate and gamePostDatetime > currentDate - timedelta(hours=1):
			output = getGameByID(str(game['id']))
			if output is None:
				log.debug("Posting thread for game: "+game['id'])
				threadID = sub.submit("Game thread: {0} vs. {1} [{2}]".format(game['home']['nameRaw'], game['away']['nameRaw'], game['startTime']), "")
				postGame(str(game['id']), str(threadID))

		if 'finalMessage' in game and game['finalMessage'] == "Final":
			finalGames.add(str(game['id']))

	for game in getGames():
		gamePostDatetime = datetime.strptime(game['date'], "%Y-%m-%d %H:%M:%S")
		if game['gameid'] in finalGames or gamePostDatetime > currentDate - timedelta(hours=8):
			log.debug("Deleting final game: "+game['gameid'])
			r.submission(id=game['threadid']).delete()
			markGameDeleted(game['gameid'])

	log.debug("Run complete after: %d", int(time.perf_counter() - startTime))
	if once:
		break
	time.sleep(LOOP_TIME)
