# rename file with file.copy
ls | xargs -n 1 -I% mv % %.copy

# split multiple files according to lines
# -a: 4 bits  -d use number as partition name -l based on lines - means input part-0 is the suffix
cat * | split -a 4 -d -l 100000 - part-0

# sum integer
awk '{s+=$1} END {print s}'

