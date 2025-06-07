mkdir -p runs

for i in {1,2,3}; do
  python G11HW3.py 8886 100000 9 100 10 >runs/8886_100000_9_100_10_run${i}.txt
  python G11HW3.py 8886 100000 9 100 20 >runs/8886_100000_9_100_20_run${i}.txt
  python G11HW3.py 8886 100000 9 100 50 >runs/8886_100000_9_100_50_run${i}.txt
  python G11HW3.py 8886 100000 9 100 100 >runs/8886_100000_9_100_100_run${i}.txt

  python G11HW3.py 8886 100000 9 5 10 >runs/8886_100000_9_5_10_run${i}.txt
  python G11HW3.py 8886 100000 9 10 10 >runs/8886_100000_9_10_10_run${i}.txt
  python G11HW3.py 8886 100000 9 20 10 >runs/8886_100000_9_20_10_run${i}.txt
  python G11HW3.py 8886 100000 9 50 10 >runs/8886_100000_9_50_10_run${i}.txt
done
