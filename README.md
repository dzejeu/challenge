Docker image for this app contains already initialized database.
To run app simply type
`docker run -p 4000:80 dzejeu/logindex:v-1.0`
and visit `localhost:4000` in your browser.

In order to initialize database by yourself see
`python init_db.py --help`
