
#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  5 13:48:20 2019
@author: zhanglemei and peng
"""

import json
import os
import pandas as pd
import numpy as np
import ExplicitMF as mf

from sklearn.feature_extraction.text import TfidfVectorizer

from sklearn.metrics.pairwise import linear_kernel

import matplotlib.pyplot as plt
import seaborn as sns
sns.set()


def load_data(path):
    """
        Load events from files and convert to dataframe.
    """
    map_lst=[]
    for f in os.listdir(path):
        file_name=os.path.join(path,f)
        if os.path.isfile(file_name):
            for line in open(file_name):
                obj = json.loads(line.strip())
                if not obj is None:
                    map_lst.append(obj)
    return pd.DataFrame(map_lst) 

def statistics(df):
    """
        Basic statistics based on loaded dataframe
    """
    total_num = df.shape[0]
    
    print("Total number of events (front page incl.): {}".format(total_num))
    df.sort_values(by=['userId', 'time'], ascending=True, inplace=True)
    df_ref = df[df['documentId'].notnull()]
    num_act = df_ref.shape[0]
    
    print("Total number of events (without front page): {}".format(num_act))
    num_docs = df_ref['documentId'].nunique()
    
    print("Total number of documents: {}".format(num_docs))
    print('Sparsity: {:4.3f}%'.format(float(num_act) / float(1000*num_docs) * 100))
    df_ref = df_ref.drop_duplicates(subset=['userId', 'documentId']).reset_index(drop=True)
    print("Total number of events (drop duplicates): {}".format(df_ref.shape[0]))
    print('Sparsity (drop duplicates): {:4.3f}%'.format(float(df_ref.shape[0]) / float(1000*num_docs) * 100))
    
    user_df = df_ref.groupby(['userId']).size().reset_index(name='counts')
    print("\nDescribe by user:")
    print(user_df.describe())
        
def load_dataset(df):
    """
        Convert dataframe to user-item-interaction matrix, which is used for 
        Matrix Factorization based recommendation.
        In rating matrix, clicked events are refered as 1 and others are refered as 0.
    """
    df = df[~df['documentId'].isnull()]
    df = df.drop_duplicates(subset=['userId', 'documentId']).reset_index(drop=True)
    df = df.sort_values(by=['userId', 'time'])
    n_users = df['userId'].nunique()
    n_items = df['documentId'].nunique()

    ratings = np.zeros((n_users, n_items))
    new_user = df['userId'].values[1:] != df['userId'].values[:-1]
    new_user = np.r_[True, new_user]
    df['uid'] = np.cumsum(new_user)
    item_ids = df['documentId'].unique().tolist()
    new_df = pd.DataFrame({'documentId':item_ids, 'tid':range(1,len(item_ids)+1)})
    df = pd.merge(df, new_df, on='documentId', how='outer')
    df_ext = df[['uid', 'tid']]
    
    for row in df_ext.itertuples():
        ratings[row[1]-1, row[2]-1] = 1.0
    return ratings
    
def train_test_split(ratings, fraction=0.2):
    """Leave out a fraction of dataset for test use"""
    test = np.zeros(ratings.shape)
    train = ratings.copy()
    for user in range(ratings.shape[0]):
        size = int(len(ratings[user, :].nonzero()[0]) * fraction)
        test_ratings = np.random.choice(ratings[user, :].nonzero()[0], 
                                        size=size, 
                                        replace=False)
        train[user, test_ratings] = 0.
        test[user, test_ratings] = ratings[user, test_ratings]
    return train, test

def evaluate(pred, actual, k):
    """
    Evaluate recommendations according to recall@k and ARHR@k
    """
    total_num = len(actual)
    tp = 0.
    arhr = 0.
    for p, t in zip(pred, actual):
        if t in p:
            tp += 1.
            arhr += 1./float(p.index(t) + 1.)
    recall = tp / float(total_num)
    arhr = arhr / len(actual)
    print("Recall@{} is {:.4f}".format(k, recall))
    print("ARHR@{} is {:.4f}".format(k, arhr))
    

def content_processing(df):
    """
        Remove events which are front page events, and calculate cosine similarities between
        items. Here cosine similarity are only based on item category information, others such
        as title and text can also be used.
        Feature selection part is based on TF-IDF process.
    """
    df = df[df['documentId'].notnull()]
    df = df.drop_duplicates(subset=['userId', 'documentId']).reset_index(drop=True)
    df['category'] = df['category'].str.split('|')
    df['category'] = df['category'].fillna("").astype('str')
    
    item_ids = df['documentId'].unique().tolist()
    new_df = pd.DataFrame({'documentId':item_ids, 'tid':range(1,len(item_ids)+1)})
    df = pd.merge(df, new_df, on='documentId', how='outer')
    df_item = df[['tid', 'category']].drop_duplicates(inplace=False)
    df_item.sort_values(by=['tid', 'category'], ascending=True, inplace=True)
    
    # select features/words using TF-IDF 
    tf = TfidfVectorizer(analyzer='word',ngram_range=(1, 2),min_df=0)
    tfidf_matrix = tf.fit_transform(df_item['category'])
    print('Dimension of feature vector: {}'.format(tfidf_matrix.shape))
    # measure similarity of two articles with cosine similarity
    
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
    
    print("Similarity Matrix:")
    print(cosine_sim[:4, :4])
    return cosine_sim, df

def content_recommendation(df, k=20):
    """
        Generate top-k list according to cosine similarity
    """
    cosine_sim, df = content_processing(df)
    df = df[['userId','time', 'tid', 'title', 'category']]
    df.sort_values(by=['userId', 'time'], ascending=True, inplace=True)
    print(df[:20]) # see how the dataset looks like
    pred, actual = [], []
    puid, ptid1, ptid2 = None, None, None
    for row in df.itertuples():
        uid, tid = row[1], row[3]
        if uid != puid and puid != None:
            idx = ptid1
            sim_scores = list(enumerate(cosine_sim[idx]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            sim_scores = sim_scores[1:k+1]
            sim_scores = [i for i,j in sim_scores]
            pred.append(sim_scores)
            actual.append(ptid2)
            puid, ptid1, ptid2 = uid, tid, tid
        else:
            ptid1 = ptid2
            ptid2 = tid
            puid = uid
    
    evaluate(pred, actual, k)
    
    
def collaborative_filtering(df):
    # get rating matrix
    ratings = load_dataset(df)
    # split ratings into train and test sets
    train, test = train_test_split(ratings, fraction=0.2)
    # train and test model with matrix factorization
    mf_als = mf.ExplicitMF(train, n_factors=40, 
                           user_reg=0.0, item_reg=0.0)
    iter_array = [1, 2, 5, 10, 25, 50, 100]
    mf_als.calculate_learning_curve(iter_array, test)
    # plot out learning curves
    plot_learning_curve(iter_array, mf_als)
    

def plot_learning_curve(iter_array, model):
    """ Plot learning curves """
    plt.plot(iter_array, model.train_mse, \
             label='Training', linewidth=5)
    plt.plot(iter_array, model.test_mse, \
             label='Test', linewidth=5)

    plt.xticks(fontsize=16);
    plt.yticks(fontsize=16);
    plt.xlabel('iterations', fontsize=20);
    plt.ylabel('MSE', fontsize=20);
    plt.legend(loc='best', fontsize=18);
    plt.show()
    

if __name__ == '__main__':
    df=load_data("active1000")
    
    ###### Get Statistics from dataset ############
    print("\nBasic statistics of the dataset...")
    statistics(df)
    
    ###### Recommendations based on Collaborative Filtering (Matrix Factorization) #######
    print("\nRecommendation based on MF...")
    collaborative_filtering(df)
    
    ###### Recommendations based on Content-based Method (Cosine Similarity) ############
    print("\nRecommendation based on content-based method...")
    content_recommendation(df, k=20)