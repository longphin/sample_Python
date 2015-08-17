#-*-coding: utf-8 -*-

__author__ = 'Longphi Nguyen'
# Reference:
# Marcel Caraciolo http://aimotion.blogspot.com/2012/08/introduction-to-recommendations-with.html

# This code takes input of form 'string, number, number',
# e.g. user, itemID, rating

# 1. List the ratings a user gives as follows:
# Mapper 1: [key, value]=[user, (item, rating)]
# Reducer 1: [key, value]=[user, (item, rating) pairs]
#            Also does a little data cleaning. If an item has multiple ratings from the same user,
#            the average of the ratings is used. (The reference did not do this)

# 2. Calculate the correlations between 2 items, only using users that
# have rated both items. Only need to calculate for itemID.1 < itemID.2,
# since correlation is symmetric. The reference does not use this symmetry.
# Reducer 2: [key, value]=[(item pairs), (rating pairs)]
# Mapper 2: [key, value]=[(item pairs), correlation]

# 3. Sort (item1, item*) based on correlation

from mrjob.job import MRJob
from math import sqrt
import time

try:
    from itertools import combinations
except ImportError:
    from metrics import combinations


PRIOR_COUNT = 10
PRIOR_CORRELATION = 0


class SemicolonValueProtocol(object):
    def write(self, key, values):
        return ','.join(str(v) for v in values) #key, values


class Similarities(MRJob):

    OUTPUT_PROTOCOL = SemicolonValueProtocol

    def steps(self):
        return [
            self.mr(mapper=self.mapper1, combiner=self.combiner1, reducer=self.reducer1),
            self.mr(mapper=self.mapper2, reducer=self.reducer2),
            self.mr(mapper=self.mapper3, reducer=self.reducer3)]

#
# ~~==| MapReduce1 |==~~
#
# Create a (user, {(item, rating)}) tuple, where the user has rated the item
    def mapper1(self, key, line):
        user, item, rating = line.split('|')
        yield (user, item), float(rating)

    # If any user rated an item more than once, take the average of the ratings.
    def combiner1(self, user_item, ratings):
        n=0 # get length of ratings
        ratingSum=0
        for rating in ratings:
            n+=1
            ratingSum+=rating

        mean=ratingSum/n
        yield user_item[0], (user_item[1], mean)

    def reducer1(self, user, pairs):
        pairsList = [] # A list of (item, rating from this user) pairs

        for item, rating in pairs:
            pairsList.append((item, rating))

        yield user, pairsList

#
# ~~==| MapReduce2 |==~~
#
# Calculate correlation of ratings between (item1, item2).
# Create (item1, item2, correlation) tuples.
    def mapper2(self, user, pairs):
        for pair1, pair2 in combinations(pairs, 2):
            yield (pair1[0], pair2[0]), (pair1[1], pair2[1]) # (item1, item2), (rating1, rating2)

    def reducer2(self, item_pair, rating_pair):
        sum1, sum2, sum1_sq, sum2_sq, sum12 = 0.0, 0.0, 0.0, 0.0, 0.0
        n=0

        for rating1, rating2 in rating_pair:
            sum1 += rating1
            sum2 += rating2
            sum1_sq += rating1*rating1
            sum2_sq += rating2*rating2
            sum12 += rating1*rating2
            n+=1

        # Calculate correlation, using the form without means to avoid having to
        # calculate the means via a map/reduce before this one.
        denominator = sqrt(n*sum1_sq - sum1*sum1) * sqrt(n*sum2_sq - sum2*sum2)
        if denominator:
            correlation = (n*sum12 - sum1*sum2)/denominator
        else: # The denominator isn't valid, so set correlation=0
            correlation = 0.0

        yield item_pair, correlation

#
# ~~==| MapReduce3 |==~~
#
# For (item1, item*) pairs, sort by correlation. Where item* = (all items != item1) s.t.
# (item*, item1) correlation has not already been calculated.
# Create (item1, item*, correlation) tuples, sorted by increasing correlation.
    def mapper3(self, item_pair, correlation):
        yield (item_pair[0], correlation), item_pair[1]

    def reducer3(self, item1_corr, item2List):
        for item2 in item2List:
            yield None, (item1_corr[0], item2, item1_corr[1])

if __name__ == '__main__':
    t0=time.clock()
    Similarities.run()
    print time.clock()-t0
