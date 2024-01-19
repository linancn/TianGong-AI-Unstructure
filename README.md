
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
sudo apt install -y tesseract-ocr
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

## OCR model path

https://github.com/tesseract-ocr/tessdata/blob/main/chi_sim.traineddata
/usr/share/tesseract-ocr/4.00/tessdata