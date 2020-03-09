# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
ARG BASE_CONTAINER=blaaast/scipy-notebook-gpu
FROM $BASE_CONTAINER

LABEL maintainer="Tom Coquereau <tom@metriq.io>"

# Install Tensorflow, uses cuda 9.2
RUN jupyter labextension install jupyterlab-plotly@1.5.2 --no-build && \
    jupyter labextension install plotlywidget@1.5.2 --no-build && \
    jupyter lab build && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

RUN pip install --quiet \
    'tensorflow==2.1.0' \
    'autogluon' \
    'minio==5.0.7'

ADD ./executhor /home/jovyan/executhor
ADD ./template.ipynb /home/jovyan/work/template.ipynb

# Install Pytorch, uses cuda 9.0, or 10.0
# RUN conda install pytorch torchvision cudatoolkit=10.0 -c pytorch
