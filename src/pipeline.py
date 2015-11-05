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
    '''Extraer texto de los PDFs de un libro,
    pegarlos en un solo archivo en formato crudo
    y guardar el resultado'''
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

if __name__ == '__main__':
    luigi.run()