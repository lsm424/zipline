ps -ef|grep python|grep main.py|awk '{print $2}'|xargs -I {} kill -9 {}
python3 gen_data.py
python3 main.py