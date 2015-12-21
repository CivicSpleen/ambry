
FROM civicknowledge/ambry

MAINTAINER Eric Busboom "eric@civicknowledge.org"

RUN pip install gunicorn==18 # I can't get 19 to run ...

EXPOSE 80

# gunicorn -w 4 -b 0.0.0.0:80 ambry.ui:app
# THe --max-requests isn't really necessary, but does help with reducing number of
# idel, open connections.
CMD ["gunicorn", "-w", "4", "--max-requests", "10", "-b", "0.0.0.0:80", "ambry.ui:app"]