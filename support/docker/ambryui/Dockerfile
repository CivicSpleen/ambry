
FROM civicknowledge/ambry

MAINTAINER Eric Busboom "eric@civicknowledge.org"

RUN pip install gunicorn==18 # I can't get 19 to run ...

EXPOSE 80

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:80", "ambry.ui:app"]