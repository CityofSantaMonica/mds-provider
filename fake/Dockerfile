FROM jupyter/scipy-notebook:137a295ff71b

WORKDIR /home/$NB_USER/work
COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip && \
    pip install -r ./requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER