# JupyterLab on top of the *same* Spark image the cluster runs.
#
# Why not quay.io/jupyter/pyspark-notebook? It ships its own Spark version, and a
# Spark client refuses to talk to a master/worker of a different version. Building
# from the cluster's image guarantees driver, master and executors agree, and gives
# us spark-submit/spark-class for free.
ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

# jupyterlab for the UI, nbconvert so notebooks can be executed headlessly,
# pandas/pyarrow for the usual "collect to a dataframe" experiments.
RUN pip3 install --no-cache-dir \
      "jupyterlab>=4,<5" \
      nbconvert \
      ipykernel \
      pandas \
      pyarrow

# The Spark image ships PySpark as plain files under /opt/spark/python and does not
# put them on the default path, so wire the import path up here.
ENV HOME=/home/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PATH=/usr/local/bin:/opt/spark/bin:${PATH}

# HOME must be writable: uid 185 is "spark", whose passwd home is /nonexistent.
RUN mkdir -p /home/spark /opt/notebooks \
 && chown -R 185:185 /home/spark /opt/notebooks

USER spark
WORKDIR /opt/notebooks

# Copies any example notebooks into the workspace on first start (never overwrites).
COPY --chmod=755 start.sh /usr/local/bin/start-jupyter.sh
ENTRYPOINT ["/usr/local/bin/start-jupyter.sh"]

EXPOSE 8888

# Empty token: this stack is deliberately unauthenticated (see README). XSRF checks
# are meaningless without auth and only get in the way of scripting the REST API.
CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.token=", "--ServerApp.password=", \
     "--ServerApp.disable_check_xsrf=True", \
     "--ServerApp.root_dir=/opt/notebooks"]
