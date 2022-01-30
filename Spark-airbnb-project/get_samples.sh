for file in data/*; do
  filename=$(echo "${file##*/}")
  head -50 $file > $filename
done 
