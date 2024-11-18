
# TianGong AI Unstructure

## Env Preparing

### Using VSCode Dev Contariners

[Tutorial](https://code.visualstudio.com/docs/devcontainers/tutorial)

Python 3 -> Additional Options -> 3.12-bullseye -> ZSH Plugins (Last One) -> Trust @devcontainers-contrib -> Keep Defaults

Setup `venv`:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

Install requirements:

```bash
python.exe -m pip install --upgrade pip

pip install --upgrade pip

pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt --upgrade
```

```bash
sudo apt update
sudo apt install python3.12-dev
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
tar -xzvf 1.84.1.tar.gz
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
https://github.com/tesseract-ocr/tessdata/blob/main/chi_tra.traineddata
https://github.com/tesseract-ocr/tessdata/blob/main/eng.traineddata

/usr/local/share/tessdata/

### check the language models currently in use
``` bash
tesseract --list-langs
```

## Run in Background
```bash
watch -n 1 nvidia-smi
find processed_docs/esg_txt/ -type f | wc -l
ls -lt processed_docs/esg_txt/ | head -n 10

nohup .venv/bin/python3.12 src/journals/chunk_by_title_sci.py > log.txt 2>&1 &
pkill -f src/journals/chunk_by_title_sci.py

CUDA_VISIBLE_DEVICES=2 nohup .venv/bin/python3.12 src/esg/1_chunk_by_title.py > esg_unstructured.log 2>&1 &
CUDA_VISIBLE_DEVICES=2 nohup .venv/bin/python3.12 src/esg/3_chunk_by_title_pages.py > esg_meta_unstructured.log 2>&1 &

CUDA_VISIBLE_DEVICES=0 nohup .venv/bin/python3.12 src/esg/1_chunk_by_title_0.py > esg_unstructured_0.log 2>&1 &
CUDA_VISIBLE_DEVICES=1 nohup .venv/bin/python3.12 src/esg/1_chunk_by_title_1.py > esg_unstructured_1.log 2>&1 &
CUDA_VISIBLE_DEVICES=2 nohup .venv/bin/python3.12 src/esg/1_chunk_by_title_2.py > esg_unstructured_2.log 2>&1 &
CUDA_VISIBLE_DEVICES=3 nohup .venv/bin/python3.12 src/esg/1_chunk_by_title_3.py > esg_unstructured_3.log 2>&1 &

pkill -f src/esg/1_chunk_by_title_0.py
pkill -f src/esg/1_chunk_by_title_1.py
pkill -f src/esg/1_chunk_by_title_2.py
pkill -f src/esg/1_chunk_by_title_3.py



nohup .venv/bin/python3.12 src/esg/2_embedding_init.py > esg_embedding_log.txt 2>&1 &

nohup .venv/bin/python3.12 src/standards/1_chunk_by_title.py > log.txt 2>&1 &

nohup .venv/bin/python3.12 src/reports/1_chunk_by_title.py > log.txt 2>&1 &
nohup .venv/bin/python3.12 src/reports/2_embedding_init.py > log.txt 2>&1 &

nohup .venv/bin/python3.12 src/education/4_pickle_to_pinecone.py > log.txt 2>&1 &

##standards
nohup .venv/bin/python3.12 src/standards/3_pickle_to_pinecone.py &
nohup .venv/bin/python3 src/standards/3_pickle_to_opensearch_aws.py > standard_opensearch_aws_log.txt 2>&1 &

##reports
nohup .venv/bin/python3.12 src/reports/3_pickle_to_pinecone.py &
nohup .venv/bin/python3.12 src/reports/3_pickle_to_opensearch.py &
nohup .venv/bin/python3 src/reports/3_pickle_to_opensearch_aws.py > report_opensearch_aws_log.txt 2>&1 &
pkill -f src/reports/3_pickle_to_opensearch.py
pkill -f src/reports/3_pickle_to_opensearch_aws.py


##esg
nohup .venv/bin/python3.12 src/esg/3_pickle_to_opensearch.py > esg_opensearch_log.txt 2>&1 &
nohup .venv/bin/python3.12 src/esg/3_pickle_to_pinecone.py > esg_pinecone_log.txt 2>&1 &
nohup .venv/bin/python3 src/esg/3_pickle_to_opensearch_aws.py > esg_opensearch_aws_log.txt 2>&1 &

pkill -f src/esg/3_pickle_to_pinecone.py
pkill -f src/esg/3_pickle_to_opensearch.py


##journal
CUDA_VISIBLE_DEVICES=0 nohup .venv/bin/python3.12 src/journals/chunk_by_title_sci_0.py > journal_pinecone_0.log 2>&1 &
CUDA_VISIBLE_DEVICES=1 nohup .venv/bin/python3.12 src/journals/chunk_by_title_sci_1.py > journal_pinecone_1.log 2>&1 &
CUDA_VISIBLE_DEVICES=2 nohup .venv/bin/python3.12 src/journals/chunk_by_title_sci_2.py > journal_pinecone_2.log 2>&1 &
CUDA_VISIBLE_DEVICES=3 nohup .venv/bin/python3.12 src/journals/chunk_by_title_sci_3.py > journal_pinecone_3.log 2>&1 &

pkill -f src/journals/chunk_by_title_sci_0.py
pkill -f src/journals/chunk_by_title_sci_1.py
pkill -f src/journals/chunk_by_title_sci_2.py
pkill -f src/journals/chunk_by_title_sci_3.py


find processed_docs/journal_pickle/ -type f | wc -l
ls -ltR processed_docs/journal_pickle/ | head -n 10

nohup .venv/bin/python3 src/journals/2_pickle_to_pinecone_aws.py > journal_pinecone_aws_Oct31_log.txt 2>&1 &

nohup .venv/bin/python3 src/patents/1_pickle_2_pinecone.py > patents_2_pinecone_log.txt 2>&1 &
nohup .venv/bin/python3 src/patents/1_pickle_2_opensearch_aws.py > patents_2_opensearch_log.txt 2>&1 &

nohup .venv/bin/python3 src/edu_textbooks/pickle_to_pinecone_aws.py > textbook_pinecone_log.txt 2>&1 &
nohup .venv/bin/python3 src/edu_textbooks/pickle_to_opensearch_aws.py > textbook_opensearch_log.txt 2>&1 &
```
