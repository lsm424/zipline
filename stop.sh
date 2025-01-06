ps -ef|grep python|grep gen_data.py|awk '{print $2}'|xargs -I {} kill -9 {}
python3 gen_data.py
python3 main.py