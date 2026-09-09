# JupyterLab + PySpark, build TRÊN CHÍNH image Spark của cụm (custom/spark:3.5.0)
# => Python 3.9 + Spark 3.5.0 + Scala 2.12 khớp 100% với spark-master/worker
#    nên notebook nối thẳng vào cụm standalone spark://spark-master:7077 chạy được.
FROM custom/spark:3.5.0

USER root

# JupyterLab + vài thư viện data hay dùng. Ghim version cho ổn định.
RUN pip3 install --no-cache-dir \
      jupyterlab==4.2.5 \
      notebook==7.2.2 \
      ipywidgets==8.1.5 \
      pandas==2.2.2 \
      pyarrow==16.1.0 \
      matplotlib==3.9.2

# Dùng pyspark có sẵn trong image (không cài lại qua pip cho nhẹ).
ENV PYTHONPATH=/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# spark-defaults: cấu hình để executor kết nối ngược lại được driver (notebook).
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

RUN mkdir -p /home/jovyan/work && chmod -R 777 /home/jovyan
WORKDIR /home/jovyan/work

EXPOSE 8888 4040

# Bỏ qua entrypoint gốc của image Spark (nó tưởng đang chạy master/worker).
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--ServerApp.root_dir=/home/jovyan/work"]
