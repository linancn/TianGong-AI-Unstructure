
# TianGong AI Unstructure

## Env Preparing

### Using VSCode Dev Contariners

[Tutorial](https://code.visualstudio.com/docs/devcontainers/tutorial)

Python 3 -> Additional Options -> 3.11-bullseye -> ZSH Plugins (Last One) -> Trust @devcontainers-contrib -> Keep Defaults

Setup `venv`:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Install requirements:

```bash
python.exe -m pip install --upgrade pip

pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt --upgrade
```

```bash
sudo apt update
sudo apt install python3.11-dev
sudo apt install -y libmagic-dev
sudo apt install -y poppler-utils
sudo apt install -y libreoffice
sudo apt install -y pandoc
```

Test Cuda (optional):

```bash
nvidia-smi
```

### Auto Build

The auto build will be triggered by pushing any tag named like release-v$version. For instance, push a tag named as v0.0.1 will build a docker image of 0.0.1 version.

```bash
#list existing tags
git tag
#creat a new tag
git tag v0.0.1
#push this tag to origin
git push origin v0.0.1
```

### sphinx

```bash
sphinx-apidoc --force -o sphinx/source/ src/
sphinx-autobuild sphinx/source docs/
```

### Docker Manually Build

```bash
docker build -t linancn/tiangong-ai-unstructure:v0.0.1 .
docker push linancn/tiangong-ai-unstructure:v0.0.1
```

### Nginx config

default file location: /etc/nginx/sites-enabled/default

```bash
sudo apt update
sudo apt install nginx
sudo nginx
sudo nginx -s reload
sudo nginx -s stop
```

## Update the verison of tesseract in WSL Shell
### remove the old version and add the necessary libraries
```bash
sudo apt-get remove tesseract-ocr
sudo apt-get install libpng-dev libjpeg-dev libtiff-dev libgif-dev libwebp-dev libopenjp2-7-dev zlib1g-dev
```
### get the latest version of leptonica by running the following code in sequence 
```bash
cd
wget https://github.com/DanBloomberg/leptonica/archive/refs/tags/1.84.1.tar.gz
tar -xzvf leptonica-1.84.1.tar.gz
cd leptonica-1.84.1
mkdir build
cd build
sudo snap install cmake # get the cmake version 3.28.3 #
cmake ..
make -j`nproc`
sudo make install
```
### get the latest version of tesseract by running the following code in sequence 
``` bash
cd
wget https://github.com/tesseract-ocr/tesseract/archive/refs/tags/5.3.4.tar.gz
tar -xzvf 5.3.4.tar.gz
cd tesseract-5.3.4
mkdir build
cd build
cmake ..
make -j `nproc`
sudo make install
```
### set environment variables
``` bash
cd
nano ~/.bashrc
```
#### add the following content at the end of the file，save the file(Ctrl-O) and exit(Ctrl-X)
export TESSDATA_PREFIX=/usr/local/share/tessdata
#### activate the settting
``` bash
source ~/.bashrc
```
### get language models
https://github.com/tesseract-ocr/tessdata/blob/main/chi_sim.traineddata
https://github.com/tesseract-ocr/tessdata/blob/main/eng.traineddata
/usr/share/tesseract-ocr/4.00/tessdata
### check the language models currently in use
``` bash
tesseract --list-langs
```

## Run in Background
```bash
nohup .venv/bin/python3.11 src/journals/chunk_by_title_sci.py > log.txt 2>&1 &

CUDA_VISIBLE_DEVICES=2 nohup .venv/bin/python3.11 src/esg/1_chunk_by_title.py > esg_unstructured.log 2>&1 &
nohup .venv/bin/python3.11 src/esg/2_embedding_init.py > esg_embedding_log.txt 2>&1 &

nohup .venv/bin/python3.11 src/standards/1_chunk_by_title.py > log.txt 2>&1 &

nohup .venv/bin/python3.11 src/reports/1_chunk_by_title.py > log.txt 2>&1 &
nohup .venv/bin/python3.11 src/reports/2_embedding_init.py > log.txt 2>&1 &

```