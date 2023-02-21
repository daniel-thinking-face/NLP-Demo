# Imports

import pandas as pd
import numpy as np

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

import ast

from collections import Counter

# Spacy
import spacy
from spacy.attrs import LOWER, POS, ENT_TYPE, IS_ALPHA
from spacy.tokens import Doc

import en_core_web_sm


# Define cvlass
class MCW():

    def __init__(self, output_stem = '', text_col = 'contents', output_number = 100, max_word_phrases = 3,
                 additional_stop_words = [],
                 pos_dict = {'nouns' : 'NOUN','adjectives' : 'ADJ','verbs' : 'VERB',
                               'adverbs' : 'ADV'}):

        # Set attributes
        self.output_stem = output_stem
        self.text_col = text_col
        self.output_number = output_number
        self.max_word_phrases = max_word_phrases
        self.additional_stop_words = additional_stop_words
        self.pos_dict = pos_dict
        self.nlp = en_core_web_sm.load()

    def token_pos_grabber(self, doc, part_of_speech):

        indexes = []

        for index,token in enumerate(doc):
            if token.pos_ == part_of_speech:
                indexes.append(index)

        doc2 = Doc(doc.vocab, words=[t.text for i, t in enumerate(doc) if i in indexes])

        doc3 = ' '.join(token.string for token in doc2)

        return doc3


    def fit_transform(self, df, nlp_transform = True):

        # Now get dataframe
        self.df = df

        #check
        if nlp_transform == True:

            # Now tokenize
            self.df['tokenized'] = self.df[self.text_col].map(lambda x: self.nlp(x))

            # Now loop through and populate the df with new columns
            for key in self.pos_dict:

                # Add column
                self.df[key] = self.df.tokenized.map(lambda x: self.token_pos_grabber(x, self.pos_dict[key]))

    # Functions to get word combinations
    def most_common_words(self, text_column, word_range):

        # stop words
        add_stop_words = ENGLISH_STOP_WORDS.union(self.additional_stop_words)

        # Fill with balnk spaces
        self.df[text_column] = self.df[text_column].fillna('')

        X = self.df[text_column].values.astype(str)

        vect = CountVectorizer(stop_words=add_stop_words, ngram_range= (word_range,word_range))
        X = vect.fit_transform(X)

        word_counts = list(zip(vect.get_feature_names(),np.asarray(X.sum(axis=0)).ravel()))

        columns = ['word', 'count']
        word_counts = pd.DataFrame(word_counts, columns=columns)

        word_counts = word_counts.sort_values(by='count', ascending=False)

        return word_counts

    def phrase_counter(self, col, word_range):

        '''
        This method takes in the column to parse and the word range.

        You can also set the additional stop words'''


        # Change keyword argument
        stop_status = ENGLISH_STOP_WORDS.union(self.additional_stop_words)

        # Initiate the vectoriser

        # Use kwargs min_word_range, max_word_range and reference stop words
        vect = CountVectorizer(ngram_range=(word_range, word_range), stop_words=stop_status)

        # Fill with balnk spaces
        self.df[col] = self.df[col].fillna('')

        # Add in space before join
        self.df[col] = self.df[col] + ' '


        # Just into one big string
        summaries = ''.join(self.df[col])
        ngram_summaries = vect.build_analyzer()(summaries)

        # Now generate output - use output number here
        count_list = Counter(ngram_summaries).most_common(self.output_number)

        wordcount_dict = {'word' : [],
                         'count' : []}

        # Loop through the count_list
        for element in count_list:

            # Append the phrases and counts to dict
            wordcount_dict['word'].append(element[0])
            wordcount_dict['count'].append(element[1])

        # Output a dataframe
        count_df = pd.DataFrame(wordcount_dict)

        # Just so look pretty
        count_df = count_df[['word', 'count']]

        return count_df

    # Now a method to actually make the word counts
    def make_word_counts(self):

        '''
        This method will actually build the word counts for each column and word count
        '''

        self.output_dict = {}

        # Make columns to parse and check they are there
        self.cols_to_parse = [self.text_col] + [c for c in self.df.columns if c in list(self.pos_dict.keys())]

        # loop through
        for col in self.cols_to_parse:

            # make a new level
            self.output_dict[col] = {}

            # Now get the various word counts
            for i in range(1, self.max_word_phrases + 1):

                # check output number
                if self.output_number == None:

                    # Populate dictionary
                    self.output_dict[col][i] = self.most_common_words(col, i)

                else:

                    # Populate dictionary
                    self.output_dict[col][i] = self.phrase_counter(col, i)

    def show_word_count_table(self, col, count_number):
        '''
        Displays the word count table for each word type and count combnination'''

        return self.output_dict[col][count_number]

    def update_stop_words(self, extra_stop_words, method = 'append'):

        # Condition
        if method == 'append':

            self.additional_stop_words = self.additional_stop_words + extra_stop_words

        else:

            self.additional_stop_words = extra_stop_words


    def export_to_excel(self, filename):

        '''
        This method will output to excel in standard format.

        It will output each word type to a separate sheet'''

        writer = pd.ExcelWriter(self.output_stem + filename)

        for key in self.output_dict.keys():

            # Now next level
            for word_count in self.output_dict[key].keys():

                # Save to a sheet
                self.output_dict[key][word_count].to_excel(writer, key, index=False, startcol = word_count * 3)

        writer.save()
