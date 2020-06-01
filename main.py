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
import pytz
from datetime import datetime
from datetime import timezone
from datetime import timedelta

### Config ###
LOG_FOLDER_NAME = "logs"
SUBREDDIT = "ncaaBBallStreams"
USER_AGENT = "NCAABBall (by /u/Watchful1)"
LOOP_TIME = 2 * 60
DATABASE_NAME = "database.db"
OWNER_NAME = "watchful1"

estTimezone = pytz.timezone("US/Eastern")

### Logging setup ###
LOG_LEVEL = logging.INFO
if not os.path.exists(LOG_FOLDER_NAME):
	os.makedirs(LOG_FOLDER_NAME)
LOG_FILENAME = LOG_FOLDER_NAME + "/" + "bot.log"
LOG_FILE_BACKUPCOUNT = 5
LOG_FILE_MAXSIZE = 1024 * 256

log = logging.getLogger("bot")
log.setLevel(LOG_LEVEL)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_stderrHandler = logging.StreamHandler()
log_stderrHandler.setFormatter(log_formatter)
log.addHandler(log_stderrHandler)
if LOG_FILENAME is not None:
	log_fileHandler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=LOG_FILE_MAXSIZE,
	                                                       backupCount=LOG_FILE_BACKUPCOUNT)
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
		SET Deleted = 1
		WHERE GameID = ?
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
		, user_agent=USER_AGENT)
except configparser.NoSectionError:
	log.error("User " + user + " not in praw.ini, aborting")
	sys.exit(0)

log.info("Logged into reddit as /u/" + str(r.user.me()))

while True:
	startTime = time.perf_counter()
	log.debug("Starting run")

	try:
		for message in r.inbox.unread(limit=100):
			message.mark_read()
	except Exception:
		log.warning("Exception parsing messages")
		log.warning(traceback.format_exc())

	currentDate = datetime.utcnow().replace(tzinfo=timezone.utc)
	timeslug = currentDate.astimezone(estTimezone).strftime("%Y/%m/%d")
	# https://data.ncaa.com/casablanca/scoreboard/basketball-men/d1/2018/11/06/scoreboard.json
	# http://data.ncaa.com/jsonp/scoreboard/basketball-men/d1/d1/2018/11/06/scoreboard.json
	url = "https://data.ncaa.com/casablanca/scoreboard/basketball-men/d1/" + timeslug + "/scoreboard.json"
	try:
		response = requests.get(url=url, headers={'User-Agent': USER_AGENT})
	except Exception as err:
		log.warning("API request failed")
		response = None
	finalGames = set()
	if response is None or response.status_code != 200:
		log.info("Bad status code, no games: {}".format(response.status_code))
	else:
		try:
			jsonData = json.loads(response.text)

			sub = r.subreddit(SUBREDDIT)
			for game in jsonData['games']:
				game = game['game']
				gameDatetime = datetime.utcfromtimestamp(int(game['startTimeEpoch'])).replace(tzinfo=timezone.utc)
				gamePostDatetime = gameDatetime - timedelta(hours=1)
				if gamePostDatetime < currentDate and gamePostDatetime > currentDate - timedelta(hours=1):
					output = getGameByID(str(game['gameID']))
					if output is None:
						log.info("Posting thread for game: " + game['gameID'])
						title = "Game thread: {0} vs. {1} [{2}]".format(game['home']['names']['short'],
						                             game['away']['names']['short'],
						                             gameDatetime.astimezone(estTimezone).strftime("%I:%M %p %Z"))
						if debug:
							log.debug(title)
							threadID = 'debugid'
						else:
							threadID = sub.submit(title, "")
						log.info("    Thread posted: " + str(threadID))
						postGame(str(game['gameID']), str(threadID))

				if 'finalMessage' in game and "final" in game['finalMessage'].lower():
					finalGames.add(str(game['gameID']))
		except Exception:
			log.warning("Exception posting games")
			log.warning(traceback.format_exc())

	try:
		for game in getGames():
			gamePostDatetime = datetime.strptime(game['date'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
			if game['gameid'] in finalGames or gamePostDatetime < currentDate - timedelta(hours=5):
				log.info("Deleting final game: " + game['gameid'] + " : " + game['threadid'])
				r.submission(id=game['threadid']).delete()
				markGameDeleted(game['gameid'])
	except Exception:
		log.warning("Exception deleting games")
		log.warning(traceback.format_exc())

	log.debug("Run complete after: %d", int(time.perf_counter() - startTime))
	if once:
		break
	time.sleep(LOOP_TIME)
