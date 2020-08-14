processDay=$1
echo "process data for $processDay"
nohup luigi --module run $2 --local-scheduler --processDay=$processDay >> ${processDay}_${2}.log 2>&1 &
tail -f ${processDay}_${2}.log 

