FROM ubuntu:18.04

RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y

ENV HOME /home
COPY Kommati_Para_App.py /home/Kommati_Para_App.py
STOPSIGNAL SIGTERM
WORKDIR /home

#COPY requirements.txt /home/requirements.txt
#RUN pip3 install -r requirements.txt

ENTRYPOINT ["python3"]

CMD ["Kommati_Para_App.py"]
