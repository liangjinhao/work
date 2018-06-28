# Readme
此文档使用Markdown语法编写，为了更好的阅读效果，可以使用相关Markdown阅读器

## CRF 标注规范：

1:主体1－具体的公司、股票、证券名，如美团、万科A、西南证券等；
2:主体2－行业板块名，如采掘行业、金融行业、板块等；
3:主体3－非1和2类型的，比如公司、沪深300、创业板、A股、B股等
4:指标：主要是金融指标，如流通市值、净资产收益率、每股净资产增长率等；
5:产品：如农产品、化工产品、金属非金属产品、化纤产品、有机产品、期货产品等；
6:国家地区：国家主要是中国、美国、欧洲；地区主要针对国内：大范围可分为长三角、珠三角等；小范围具体到省市自治区直辖市；
7:公式：同比、环比、增速、降速，跌速；
8:无用
9:日期

## 配置环境，安装相关库

- 首先安装 Python3.6, 确保GCC版本不低于4.8.2

```
# 安装 Python3.6
sudo apt-get install libbz2-dev # ubuntu 环境
sudo yum groupinstall 'Development Tools'  # centos 环境
sudo yum install zlib-devel bzip2-devel openssl-devel ncurese-devel  # centos 环境
sudo yum -y install readline-devel

wget https://www.python.org/ftp/python/3.6.1/Python-3.6.1.tgz
tar -zxf Python-3.6.1.tgz
cd Python-3.6.1
./configure
make
sudo make install


# GCC升级到4.8.2，这个需要系统管理员来做
wget http://ftp.gnu.org/gnu/gcc/gcc-4.8.2/gcc-4.8.2.tar.gz
tar -xvf gcc-4.8.2.tar.gz
cd gcc-4.8.2/
./contrib/download_prerequisites
mkdir gcc-build-4.8.2
cd gcc-build-4.8.2
../configure --enable-checking=release --enable-languages=c,c++ --disable-multilib
make
make  install
gcc -v
```

- 使用 pip 安装以下依赖的库

```
# 比如， sudo /usr/local/bin/pip3.6 install numpy
pip install gensim
pip install JPype1
pip install matplotlib
pip install numpy
pip install pyahocorasick
pip install pymongo
pip install PyMySQL
pip install pyspark
pip install scikit-learn
pip install scipy
pip install thrift
pip install tornado
pip install watchdog
pip install apscheduler
```

- 安装XGboost

```
$ git clone --recursive https://github.com/dmlc/xgboost
$ cd xgboost
$ make -j4
$ cd python-package
$ sudo python setup.py install
```

- 安装CRF++

```
$ tar -xvf CRF++-0.58.tar.gz
$ ./configure 
$ make
$ sudo make install
$ sudo ln -s /usr/local/lib/libcrfpp.so.* /usr/lib64/
$ cd python
$ python setup.py build 
$ sudo python setup.py install  # 这个是安装到系统默认python的site-packages下，如果你使用虚拟环境，请手动拷贝build目录下的文件过去
```

## GLIBC的问题：

### version `GLIBC_2.18' not found

解决办法
```
sudo wget http://ftp.gnu.org/pub/gnu/glibc/glibc-2.18.tar.xz
sudo xz -d glibc-2.18.tar.xz
sudo tar -xvf glibc-2.18.tar
cd glibc-2.18
sudo mkdir build
cd build
sudo ../configure --prefix=/usr --disable-profile --enable-add-ons --with-headers=/usr/include --with-binutils=/usr/bin  
sudo make && sudo make install
```

验证 
```
# 输入 `strings /lib64/libc.so.6|grep GLIBC` 看是否更新
GLIBC_2.2.5
GLIBC_2.2.6
GLIBC_2.3
GLIBC_2.3.2
GLIBC_2.3.3
GLIBC_2.3.4
GLIBC_2.4
GLIBC_2.5
GLIBC_2.6
GLIBC_2.7
GLIBC_2.8
GLIBC_2.9
GLIBC_2.10
GLIBC_2.11
GLIBC_2.12
GLIBC_2.13
GLIBC_2.14
GLIBC_2.15
GLIBC_2.16
GLIBC_2.17
GLIBC_PRIVATE
```

### version `GLIBCXX_3.4.14' not found

解决办法
```
sudo wget http://ftp.de.debian.org/debian/pool/main/g/gcc-4.7/libstdc++6_4.7.2-5_amd64.deb
sudo ar -x libstdc++6_4.7.2-5_amd64.deb&&sudo tar xvf data.tar.gz  
cd usr/lib/x86_64-linux-gnu
sudo cp libstdc++.so.6.0.17 /usr/lib64/
cd /usr/lib64/
sudo chmod +x libstdc++.so.6.0.17
sudo rm -rf libstdc++.so.6
sudo ln -s libstdc++.so.6.0.17 libstdc++.so.6
```

验证
```
# 输入 `strings /usr/lib64/libstdc++.so.6 | grep GLIBCXX` 看是否更新 
GLIBCXX_3.4
GLIBCXX_3.4.1
GLIBCXX_3.4.2
GLIBCXX_3.4.3
GLIBCXX_3.4.4
GLIBCXX_3.4.5
GLIBCXX_3.4.6
GLIBCXX_3.4.7
GLIBCXX_3.4.8
GLIBCXX_3.4.9
GLIBCXX_3.4.10
GLIBCXX_3.4.11
GLIBCXX_3.4.12
GLIBCXX_3.4.13
GLIBCXX_3.4.14
GLIBCXX_3.4.15
GLIBCXX_3.4.16
GLIBCXX_3.4.17
GLIBCXX_DEBUG_MESSAGE_LENGTH
```

### version `GLIBCXX_3.4.19' not found

解决办法
```
sudo wget http://ftp.de.debian.org/debian/pool/main/g/gcc-4.9/libstdc++6_4.9.2-10_amd64.deb
sudo ar -x libstdc++6_4.9.2-10_amd64.deb &&sudo tar xvf data.tar.xz
cd usr/lib/x86_64-linux-gnu/
sudo cp libstdc++.so.6.0.20 /usr/lib64/
cd /usr/lib64/
sudo chmod +x libstdc++.so.6.0.20
sudo rm -rf libstdc++.so.6
sudo ln -s libstdc++.so.6.0.20  ./libstdc++.so.6
```

验证
```
# 输入strings /usr/lib64/libstdc++.so.6 | grep GLIBCXX
GLIBCXX_3.4
GLIBCXX_3.4.1
GLIBCXX_3.4.2
GLIBCXX_3.4.3
GLIBCXX_3.4.4
GLIBCXX_3.4.5
GLIBCXX_3.4.6
GLIBCXX_3.4.7
GLIBCXX_3.4.8
GLIBCXX_3.4.9
GLIBCXX_3.4.10
GLIBCXX_3.4.11
GLIBCXX_3.4.12
GLIBCXX_3.4.13
GLIBCXX_3.4.14
GLIBCXX_3.4.15
GLIBCXX_3.4.16
GLIBCXX_3.4.17
GLIBCXX_3.4.18
GLIBCXX_3.4.19
GLIBCXX_3.4.20
GLIBCXX_FORCE_NEW
GLIBCXX_DEBUG_MESSAGE_LENGTH
```

## 启动
```
sudo nohup /usr/local/bin/python3.6 service.py --port=1999 --log_file_prefix=tornado_1999.log&
```
