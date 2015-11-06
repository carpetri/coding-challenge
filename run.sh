#!/usr/bin/env bash


# Make sure you have the requierements
# If not sure run:
# pip install -r requirements.txt

# This makes sure that the output is gone from the last run.
make clean
cd src/

# I'll execute my programs, with the input directory tweet_input and output the files in the directory tweet_output

	# 0. Gets the tweets. **In the future this task should run live**.
	pwd
	echo 'Geting Tweets ...'
	python pipeline.py InputTweets --filename tweets.txt --local-scheduler

	# 1. Read and clean the tweets
	echo 'Cleaning Tweets ...'
	python pipeline.py ReadTweets --tweet-dir ../tweet_input --local-scheduler

	# 2. Calculate AverageDegree
	echo 'Calculate AverageDegree'
	python pipeline.py AverageDegree --tweet-dir ../tweet_input --local-scheduler

