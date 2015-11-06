import luigi
import os
from os import listdir
import sys
import inspect
import re
import requests
from pprint import pprint
import json
import pandas as pd
import codecs
import string
from datetime import datetime, timedelta
import numpy as np
import itertools
from itertools import combinations

class InputTweets(luigi.ExternalTask):
    """
    This class represents the task to create the tweets that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    In the future this can run the get_data.py live.
    """
    filename = luigi.Parameter()

    def output(self):
        # The directory containing this file
        root = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) + "/"
        return luigi.LocalTarget(root + self.filename)

class ReadTweets(luigi.Task):
    tweet_dir = luigi.Parameter()

    def requires(self):
        #return InputTweets(os.path.join(self.tweet_dir, filename))
        return [ InputTweets(self.tweet_dir + '/' + filename)
                for filename in listdir(self.tweet_dir) ]
        
    def run(self):
        f = self.output().open('w')   
        data = []
        
        for ff in self.input():
            with ff.open('r') as data_file:    
                for line in data_file:
                    data.append(json.loads(line))
        x=0;

        for aux in data:
            if aux.get('text') and aux.get('created_at'):
                f.write( 
                filter(lambda x: x in string.printable, aux.get('text')).replace('\n',' ').replace('\t',' ')+
                ' (Timestamp: ' + 
                filter(lambda x: x in string.printable, aux.get('created_at')).replace('\n',' ') +
                ')\n')
            else:
                x=x+1

        f.write( '\n ' + str(x) +  ' tweets contained unicode.' )
        f.close()

    def output(self):
        return luigi.LocalTarget('../tweet_output/ft1.txt')

class AverageDegree(luigi.Task):
    tweet_dir = luigi.Parameter()

    def requires(self):
            return ReadTweets(self.tweet_dir)
        
    def run(self):
        g=[]
        today = datetime.strptime('Fri Oct 30 15:32:56 +0000 2015','%a %b %d %H:%M:%S +0000 %Y' )

        with codecs.open('../tweet_output/ft1.txt','rU','utf-8') as data_file:    
            for line in data_file:
                    x= {tag.strip() for tag in line.split(' (') if tag.startswith("timestamp")  }
                    ht={tag.strip("#") for tag in line.split() if tag.startswith("#")}
                    if x != set():
                        d= datetime.strptime(x.pop(),'timestamp: %a %b %d %H:%M:%S +0000 %Y)')
                        if today - d <= timedelta(0,60) and len(ht) >1:
                            dat={ 'hashtags': list(ht), 'date': d}
                            g.append(dat)
        nodes=[]
        for tweet in g:
            for ht in tweet.get('hashtags'):
                nodes.append(ht)

        links=[]
        for tweet in g:
            links.append( list(combinations(tweet.get('hashtags'),2)) )
        
        print links
        f = self.output().open('w') 
        s=0.0
        for link in links:
            s = s + len(link)
            av= s / len(links)
            f.write(  str( round(  av , 10) ) + '\n') 
        
        f.close()

    def output(self):
        return luigi.LocalTarget('../tweet_output/ft2.txt')


if __name__ == '__main__':
    luigi.run()