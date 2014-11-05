
for i in {1..50}
do 
  echo "awk '" '{ total += $2 } END { print total/NR }'"' log${i}"
done
