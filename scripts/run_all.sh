
for i in {1..40}
do 
  NUM="${i}00"; 
  # CMD="go run load.go --num_query $NUM"
  CMD="./upthere --num_query $NUM"
  echo "$CMD > log${i}"
done
