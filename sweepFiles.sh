if [ "$1" == "" ]
then
  echo "No directory specified to sweep."
  exit 4
fi


for a in `find $1 -type f -print`
do
  echo "Running $a"
  ./runDemo.sh $a &
done
