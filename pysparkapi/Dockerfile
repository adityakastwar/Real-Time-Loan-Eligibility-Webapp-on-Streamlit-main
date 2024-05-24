FROM jupyter/pyspark-notebook:latest
WORKDIR /deploy/pysparkapi
COPY ./requirements.txt /deploy/pysparkapi
RUN pip install -r requirements.txt
EXPOSE 5000
COPY ./file.pkl /deploy/pysparkapi
COPY ./run.py/ /deploy/pysparkapi
COPY ./app.py /deploy/pysparkapi
COPY ./Utils.py /deploy/pysparkapi
COPY ./helper.py /deploy/pysparkapi
COPY ./assembleModel.h5 /deploy/pysparkapi/assembleModel.h5
COPY ./char_label_model.h5 /deploy/pysparkapi/char_label_model.h5
COPY ./clf_model.h5 /deploy/pysparkapi/clf_model.h5
ENTRYPOINT ["python", "app.py"]