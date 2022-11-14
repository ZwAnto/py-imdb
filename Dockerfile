FROM python:3.8.13-slim-buster
#FROM nvidia/cuda:11.3.0-base-ubuntu20.04

RUN apt-get update && apt-get install -y python3 python3-venv python3-dev curl poppler-utils locales
# Only for ubuntu
#RUN locale-gen fr_FR.UTF-8 
RUN sed -i 's/^# *\(fr_FR.UTF-8\)/\1/' /etc/locale.gen && locale-gen

RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH /root/.local/bin:$PATH


