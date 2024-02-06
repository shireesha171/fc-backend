FROM python:3.10-slim-bullseye

WORKDIR /home

COPY ./requirements.txt  ./

RUN pip install -r requirements.txt --target ./

COPY ./main.py .

CMD [ "python main.py" ]
