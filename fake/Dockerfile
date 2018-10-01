FROM jupyter/scipy-notebook:137a295ff71b

# the first few operations require root
USER root

# use a subdirectory of the default Notebook directory
WORKDIR /home/$NB_USER/work/mds

# setup the app code and data directory
COPY . .
RUN mkdir data && \
    fix-permissions /home/$NB_USER

# now we've "fixed permissions", install pip packages as the notebook user
USER $NB_UID

# see https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#using-pip-install-or-conda-install-in-a-child-docker-image
RUN pip install -r requirements.txt && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# sets up an "executable container" for the data generation script
ENTRYPOINT ["python", "main.py"]