FROM python:3.8.13-slim-buster

RUN apt-get update && apt-get install -y locales git
# Only for ubuntu
#RUN locale-gen fr_FR.UTF-8 
RUN sed -i 's/^# *\(fr_FR.UTF-8\)/\1/' /etc/locale.gen && locale-gen

RUN python -m pip install -U pip

RUN python -m pip install git+https://github.com/ZwAnto/py-imdb.git
RUN python -m pip install elasticsearch==8.4.3 numpy==1.23.5 requests==2.28.1

WORKDIR /opt

ADD index.py .
ADD es_indices es_indices
