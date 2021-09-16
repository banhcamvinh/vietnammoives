from typing import Text
import scrapy
import re
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import event
import time
import urllib

def connectDB_pyodbc():
    print("Connecting....")
    try:
        conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=database-sql-1.ctlrg00ritgt.us-east-2.rds.amazonaws.com;'
                            'Database=vietnammovies;'
                            'UID=admin;'
                            'PWD=12345678;')
        print("Connect success")
    except Exception as Ex:
        print("Connect failed")
        print(Ex)
    return conn

def connectDB_sqlalchemy():
    print("Connecting server . . .")
    params = urllib.parse.quote_plus("DRIVER=ODBC Driver 11 for SQL Server;"
                                    "SERVER=database-sql-1.ctlrg00ritgt.us-east-2.rds.amazonaws.com;"
                                    "DATABASE=vietnammovies;"
                                    "UID=admin;"
                                    "PWD=12345678")
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    cursor.fast_executemany = True
    try:
        conn = engine.connect()
        print("Connected")
    except Exception as Ex:
        print("Connect failed")
        print(Ex)
    return conn

class Movie_Spider(scrapy.Spider):
    name = "movies"

    def start_requests(self):
        url = 'https://www.imdb.com/search/title/?country_of_origin=VN&sort=alpha,asc'  
        movies_list = [] 
        yield scrapy.Request(url, self.parse,meta={'page':1,'movies_list':movies_list})

    def parse(self, response):
        page = response.meta.get('page')
        movies_list = response.meta.get('movies_list')
        movie_el_list = response.css('.lister-item.mode-advanced')
        # ===========
        # Image - Title - Year - Duaration - Genres - Rating - Votes - Description - Director - Starts

        for movie_el in movie_el_list:
            image_url = movie_el.css('img::attr(src)').get()
            image_url = str(image_url)
            title = movie_el.css('.lister-item-header a::text').get().strip()

            year = movie_el.css('.lister-item-year.text-muted.unbold::text').get()
            if year != None: 
                year = year[1:-1]
                year = re.findall('\d{4}',year)
                if len(year) != 0:
                    year = year[0]
                else: year = None

            duration = movie_el.css('.runtime::text').get(default='0')
            duration = int(re.findall('\d+',duration)[0])

            genres = movie_el.css('.genre::text').get()
            if genres != None:
                genres = genres.strip()
                genres = genres.split(',')
            else: genres = []

            rating = movie_el.css('strong::text').get(default='0')
            rating = float(rating)
            
            vote = movie_el.css('.sort-num_votes-visible span:nth-child(2)::text').get(default='0')
            vote = vote.replace(',','')
            vote = int(vote)

            description = movie_el.css('.lister-item-content').extract()
            description = re.findall('Director.*<\/p>',str(description))

            director = None
            starts = []
            if len(description) != 0: 
                description= description[0]
                description = re.findall('\/">.{0,30}<\/a',description)

                description[0] = re.findall('>.*<',description[0])
                description[0] = description[0][0].replace("<","").replace(">","")
                index = 1
                while index < len(description):
                    description[index] = re.findall('>.*<',description[index])
                    description[index] = description[index][0].replace("<","").replace(">","")
                    starts.append(description[index])
                    index += 1
                director = description[0]   
                starts = description[1:]
            #Image - Title - Year - Duaration - Genres - Rating - Votes - Description - Director - Starts
            movie_dict = {
                "image":image_url,
                "title":title,
                "duration":duration,
                "genres": '|'.join(genres),
                "rating":rating,
                "vote":vote,
                "description": 'chua co',
                "director":director,
                "starts": '|'.join(starts)
            }
            movies_list.append(movie_dict)
            # yield movie_dict

        print("=======================Finish page "+str(page))
        next_page = response.css('.lister-page-next.next-page::attr(href)').get()
        if next_page is None:
            movies_df = pd.DataFrame(movies_list)
            db_con = connectDB_sqlalchemy()
            if db_con != None:
                movies_df.to_sql("movies",db_con,index=False,if_exists="append",schema="dbo")
                
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse,meta={'page':page+1,'movies_list':movies_list})
