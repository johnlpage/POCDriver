#!/usr/bin/env python

import csv


def linear_regression(X, Y):
    """
    return a,b in solution to y = ax + b such that root mean square distance between trend line and original points is minimized
    """
    N = len(X)
    Sx = Sy = Sxx = Syy = Sxy = 0.0
    for x, y in zip(X, Y):
        Sx = Sx + x
        Sy = Sy + y
        Sxx = Sxx + x*x
        Syy = Syy + y*y
        Sxy = Sxy + x*y
    det = Sxx * N - Sx * Sx
    return (Sxy * N - Sy * Sx)/det, (Sxx * Sy - Sx * Sxy)/det

data = csv.reader(open('results.csv', 'r'), delimiter=",", quotechar='|')
collections, latency = [], []
go=False
for row in data:
    if go == True:
        collections.append(int(row[3]))
        latency.append(int(row[5]))
    go = True

a,b = linear_regression(latency, collections)

print(a)
print(b)
