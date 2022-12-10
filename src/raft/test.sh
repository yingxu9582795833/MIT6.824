x=2
for ((i=1;i<=100;i=i+1))
do
  go test -race -run 2C >> test.log
done
if grep -i 'Fail' test.log
then
  echo 'Exist Fail'
else
  echo 'All Pass'
fi