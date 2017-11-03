# Readme
此文档使用Markdown语法编写，为了更好的阅读效果，可以使用相关Markdown阅读器

## 配置环境，安装相关库

1. 首先安装 Python3.6, 确保GCC版本不低于4.8
```
# GCC升级到4.8.2
$ wget http://gcc.skazkaforyou.com/releases/gcc-4.8.2/gcc-4.8.2.tar.gz
$ tar -xvf gcc-4.8.2.tar.gz
$ cd gcc-4.8.2/
$ ./contrib/download_prerequisites
$ mkdir gcc-build-4.8.2
$ cd gcc-build-4.8.2
$ ../configure --enable-checking=release --enable-languages=c,c++ --disable-multilib
$ make
$ make  install
$ gcc -v
```
2. 首先 pip 安装以下依赖的库
```
# 比如， sudo /usr/local/bin/pip3.6 install numpy
JPype1
gensim
matplotlib
numpy
pandas
pyahocorasick
requests -
scikit-learn
scipy
sklearn
tensorflow
tensorflow-tensorboard
tornado
```
3. 然后安装XGboost和CRF++
```
# install xgboost
$ git clone --recursive https://github.com/dmlc/xgboost
$ cd xgboost
$ make -j4
$ cd python-package
$ sudo python setup.py install

# install CRF++
$ tar -xvf CRF++-0.58.tar
$ ./configure 
$ make
$ sudo make install
$ cd python
$ python setup.py build 
$ sudo python setup.py install
```

## 已经部署的机器
|机器|IP|说明|
|-|-|-|
|bj-wh-service005|121.40.79.122|使用的是CRF++，xgboost训练的模型有word2vec，训练模型使用的分词是原始的hanlp|

## 启动
/usr/local/python3.6/bin/python service.py --port=1999 --log_file_prefix=tornado_1999.log
