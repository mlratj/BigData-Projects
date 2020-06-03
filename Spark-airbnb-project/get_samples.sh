for file in airbnb/*; do
  filename=$(echo "${file##*/}")
  head -50 $file > $filename
done 
