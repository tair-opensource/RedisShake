we need python version >=3.5 and < 3.9.

install deps & run

```shell
conda create -n redis-shake python=3.8
conda activate redis-shake 
pip install -r requirements.txt
python main.py
```

update requirements.txt:

```shell
pipreqs --force      
```

