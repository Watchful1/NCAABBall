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
LOOP_TIME = 5 * 60
DATABASE_NAME = "database.db"
OWNER_NAME = "watchful1"

estTimezone = timezone(timedelta(hours=-4))

### Logging setup ###
LOG_LEVEL = logging.DEBUG
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
c.execute('''
	CREATE TABLE IF NOT EXISTS replacements (
		ID INTEGER PRIMARY KEY AUTOINCREMENT,
		Source VARCHAR(80) NOT NULL,
		Destination VARCHAR(80) NOT NULL,
		UNIQUE (Source)
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


def getReplacement(source):
	c = dbConn.cursor()
	result = c.execute('''
		SELECT Destination
		FROM replacements
		WHERE Source = ?
	''', (source,))

	resultTuple = result.fetchone()

	if not resultTuple:
		return source
	else:
		return resultTuple[0]


def setReplacement(source, destination):
	c = dbConn.cursor()
	destination = getReplacement(source)
	if destination == source:
		try:
			c.execute('''
				INSERT INTO replacements
				(Source, Destination)
				VALUES (?, ?)
			''', (source, destination))
		except sqlite3.IntegrityError:
			return "error"

		dbConn.commit()
		return "inserted"
	else:
		try:
			c.execute('''
				UPDATE replacements
				SET Destination = ?
				WHERE Source = ?
			''', (destination, source))
		except sqlite3.IntegrityError:
			return "error"

		dbConn.commit()
		return "updated"


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
			# checks to see as some comments might be replys and non PMs
			if isinstance(message, praw.models.Message) and str(message.author).lower() == OWNER_NAME:
				log.debug("Parsing message")
				output = []
				for line in message.body.splitlines():
					fragments = line.split(":")
					if len(fragments) != 2: continue
					result = setReplacement(fragments[0], fragments[1])
					output.append("Replacement from ")
					output.append(fragments[0])
					output.append(" to ")
					output.append(fragments[1])
					output.append(" ")
					output.append(result)
					output.append("\n\n")

				log.debug(''.join(output))
				message.reply(''.join(output))
				message.mark_read()
	except Exception:
		log.warning("Exception parsing messages")
		log.warning(traceback.format_exc())

	currentDate = datetime.utcnow().replace(tzinfo=timezone.utc)
	timeslug = currentDate.astimezone(estTimezone).strftime("%Y/%m/%d")
	# http://cdn.espn.com/core/mens-college-basketball/schedule/_/date/20171218/group/50?table=true&device=desktop&country=us&lang=en&region=us&site=espn&edition-host=espn.com&one-site=true
	try:
		url = "http://data.ncaa.com/jsonp/scoreboard/basketball-men/d1/" + timeslug + "/scoreboard.html"
		wrappedJson = requests.get(url=url, headers={'User-Agent': USER_AGENT}).text
		actualJson = wrappedJson.replace("callbackWrapper(", "").strip(");")
		jsonData = json.loads(actualJson)
	except Exception:
		log.warning("Exception parsing json")
		log.warning(traceback.format_exc())

	try:
		sub = r.subreddit(SUBREDDIT)
		finalGames = set()
		for game in jsonData['scoreboard'][0]['games']:
			gameDatetime = datetime.utcfromtimestamp(int(game['startTimeEpoch'])).replace(tzinfo=timezone.utc)
			gamePostDatetime = gameDatetime - timedelta(hours=1)
			if gamePostDatetime < currentDate and gamePostDatetime > currentDate - timedelta(hours=1):
				output = getGameByID(str(game['id']))
				if output is None:
					log.debug("Posting thread for game: " + game['id'])
					threadID = sub.submit("Game thread: {0} vs. {1} [{2}]".
					                      format(getReplacement(game['home']['nameRaw']),
					                             getReplacement(game['away']['nameRaw']),
					                             gameDatetime.astimezone(estTimezone).strftime("%I:%M %p EST")), "")
					log.debug("    Thread posted: " + str(threadID))
					postGame(str(game['id']), str(threadID))

			if 'finalMessage' in game and "Final" in game['finalMessage']:
				finalGames.add(str(game['id']))
	except Exception:
		log.warning("Exception posting games")
		log.warning(traceback.format_exc())

	try:
		for game in getGames():
			gamePostDatetime = datetime.strptime(game['date'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
			if game['gameid'] in finalGames or gamePostDatetime < currentDate - timedelta(hours=8):
				log.debug("Deleting final game: " + game['gameid'] + " : " + game['threadid'])
				r.submission(id=game['threadid']).delete()
				markGameDeleted(game['gameid'])
	except Exception:
		log.warning("Exception deleting games")
		log.warning(traceback.format_exc())

	log.debug("Run complete after: %d", int(time.perf_counter() - startTime))
	if once:
		break
	time.sleep(LOOP_TIME)
