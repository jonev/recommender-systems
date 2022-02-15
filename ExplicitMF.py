#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  6 14:42:44 2019
"""

from numpy.linalg import solve
from sklearn.metrics import mean_squared_error
import numpy as np

class ExplicitMF():
    def __init__(self, ratings, n_factors=40,
                 item_reg=0.0, user_reg=0.0,
                 verbose=True):
        """
        Initialize corresponding params.
        
        Params:
            ratings: (2D array) user x item matrix with corresponding ratings.
            n_factors: (int) number of latent factors after matrix factorization.
            iterm_reg: (float) Regularization term for item latent factors.
            user_reg: (float) Regularization term for user latent factors.
            verbose: (bool) Whether or not to print out training progress.
        """
        self.ratings = ratings
        self.n_users, self.n_items = ratings.shape
        self.n_factors = n_factors
        self.item_reg = item_reg
        self.user_reg = user_reg
        self._v = verbose
        
    def als_step(self, latent_vectors, fixed_vecs,
                 ratings, _lambda, type='user'):
        """
        Alternating Least Squares for training process.
        
        Params:
            latent_vectors: (2D array) vectors need to be adjusted.
            fixed_vecs: (2D array) vectors fixed.
            ratings: (2D array) rating matrx.
            _lambda: (float) regularization coefficient. 
        """
        if type == 'user':
            YTY = fixed_vecs.T.dot(fixed_vecs)
            lambdaI = np.eye(YTY.shape[0]) * _lambda
            
            for u in range(latent_vectors.shape[0]):
                latent_vectors[u,:] = solve((YTY + lambdaI),
                              ratings[u,:].dot(fixed_vecs))
        elif type == 'item':
            XTX = fixed_vecs.T.dot(fixed_vecs)
            lambdaI = np.eye(XTX.shape[0]) * _lambda
            
            for i in range(latent_vectors.shape[0]):
                latent_vectors[i,:] = solve((XTX + lambdaI),
                              ratings[:,i].T.dot(fixed_vecs))
                
        return latent_vectors
    
    def train(self, n_iter=10):
        # initialize latent vectors for training process
        self.user_vecs = np.random.random((self.n_users, self.n_factors))
        self.item_vecs = np.random.random((self.n_items, self.n_factors))
        
        self.partial_train(n_iter)
        
    def partial_train(self, n_iter):
        """train model for n_iter iterations."""
        ctr = 1
        while ctr <= n_iter:
            if ctr % 10 == 0 and self._v:
                print("Current iteration: {}".format(ctr))
            self.user_vecs = self.als_step(self.user_vecs, self.item_vecs,
                                           self.ratings, self.user_reg,
                                           type='user')
            self.item_vecs = self.als_step(self.item_vecs, self.user_vecs,
                                           self.ratings, self.item_reg,
                                           type='item')
            
            ctr += 1
            
    def predict(self):
        """Predict ratings"""
        predictions = np.zeros((self.user_vecs.shape[0],
                                self.item_vecs.shape[0]))
        for u in range(self.user_vecs.shape[0]):
            for i in range(self.item_vecs.shape[0]):
                predictions[u,i] = self.user_vecs[u,:].dot(self.item_vecs[i, :].T)
        
        return predictions
    
    def get_mse(self, pred, actual):
        """Calculate mean squard error between actual ratings and predictions"""
        pred = pred[actual.nonzero()].flatten()
        actual = actual[actual.nonzero()].flatten()
        return mean_squared_error(pred, actual)
                
    def calculate_learning_curve(self, iter_array, test):
        """
        Keep track of MSE during train and test iterations.
        
        Params:
            iter_array: (list) List of numbers of iterations to train for each step of 
                        the learning curve.
            test: (2D array) test dataset.
        """
        iter_array.sort()
        self.train_mse = []
        self.test_mse = []
        iter_diff = 0
        for (i, n_iter) in enumerate(iter_array):
            if self._v:
                print("Iteration: {}".format(n_iter))
            if i == 0:
                self.train(n_iter - iter_diff)
            else:
                self.partial_train(n_iter - iter_diff)
            
            predictions = self.predict()
            
            self.train_mse += [self.get_mse(predictions, self.ratings)]
            self.test_mse += [self.get_mse(predictions, test)]
            if self._v:
                print("Train mse: {}".format(str(self.train_mse[-1])))
                print("Test mse: {}".format(str(self.test_mse[-1])))
            iter_diff = n_iter