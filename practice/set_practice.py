
import pandas as pd

full_dates = pd.date_range("2024-10-01", "2024-11-30")

event = pd.date_range("2024-10-08", "2024-10-11")

# # print(set3.issubset(set2)) # check if one set is a subset of another set

# # print(set1.intersection(set2))  # prints the common elements between set1 and set2

# # print(set1.symmetric_difference(set2))  # prints elements unique to set1 and set2
# # print(set1)
# # set1.update(set2)
# full_set = set1.union(set2)
# print(full_set)

print(full_dates.difference(event))
