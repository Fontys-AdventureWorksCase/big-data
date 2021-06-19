#ARG ROOT_CONTAINER=ubuntu:focal-20210609@sha256:376209074d481dca0a9cf4282710cd30a9e7ff402dea8261acdaaf57a18971dd

#ARG BASE_CONTAINER=$ROOT_CONTAINER
#FROM $BASE_CONTAINER
FROM datastax/dse-server:6.8.1

#LABEL maintainer="Jupyter Project <jupyter@googlegroups.com>"
ARG NB_USER="dse"
ARG NB_UID="1000"
ARG NB_GID="100"

# Fix DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

# Install all OS dependencies for notebook server that starts but lacks all
# features (e.g., download as all possible file formats)
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update --yes && \
    apt-get install --yes --no-install-recommends \
    tini \
    python3-pip \
    python3-setuptools \
    wget \
    ca-certificates \
    sudo \
    locales \
    fonts-liberation \
    && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && \
    locale-gen

RUN pip3 install jupyter
RUN pip3 install cassandra-driver
RUN pip3 install matplotlib
RUN pip3 install numpy
RUN pip3 install pandas

COPY start-notebook.sh /opt/dse/

RUN mkdir /opt/dse/jupyter
RUN chown dse:dse /opt/dse/jupyter

#RUN useradd -ms /bin/bash ${NB_USER}
USER ${NB_USER}
WORKDIR /opt/dse

RUN dse exec jupyter notebook --generate-config
#RUN echo "c.NotebookApp.password = ''">>~/.jupyter/jupyter_notebook_config.py
#RUN echo "c.NotebookApp.token = ''">>~/.jupyter/jupyter_notebook_config.py

#ENV JUPYTER_TOKEN ""
ENV JUPYTER_ENABLE_LAB yes
# Configure container startup
ENTRYPOINT ["dse", "exec", "jupyter","notebook","--ip=0.0.0.0","--port=8888","--NotebookApp.token=''","--NotebookApp.notebook_dir=/opt/dse/jupyter"]
