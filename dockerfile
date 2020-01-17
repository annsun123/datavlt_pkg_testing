FROM python:3.5

ENV TZ=Asia/Singapore

COPY . /pkg_datavlt_2/

WORKDIR /pkg_datavlt_2
#RUN apt-get install python3-pip

RUN pip install -r requirements.txt

ENTRYPOINT python final_main.py 

CMD ["final_main.py"]
