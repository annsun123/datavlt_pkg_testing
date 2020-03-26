FROM python:3.6

ENV TZ=Asia/Singapore

COPY . /pkg_datavlt_2/

WORKDIR /pkg_datavlt_2
#RUN apt-get install python3-pip

RUN pip install -r requirements.txt

ENTRYPOINT python final_main_api.py

CMD ["final_main_api.py"]
