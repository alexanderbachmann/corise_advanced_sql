## Facts about my query run:
1) Initially the query ran for 647 ms however, I did a second run to see if caching the query would help in this case and it did reducing the query time to 639 ms, decreasing the time slightly.
2) It looks like the window function used to get the top most popular recipe id of the day was the most expensive node according to the query profile.