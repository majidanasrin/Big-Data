numbers = LOAD '/user/hadoop/input/input.txt' USING PigStorage() AS (num:int);
sorted_numbers = ORDER numbers BY num ASC;
STORE sorted_numbers INTO '/user/hadoop/output/sorted' USING PigStorage();
