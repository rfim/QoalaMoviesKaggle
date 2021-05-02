tot_name = os.path.join(os.path.dirname(__file__),'src/data', file_name)

# open the json datafile and read it in
with open(tot_name, 'r') as inputfile:
    doc = json.load(inputfile)

# transform the data to the correct types and convert temp to celsius
id_movie        = int(doc['id'])
movie_name      = str(doc['original_title'])
year            = str(doc['production_companies']['production_countries']['release date'])
country_origin  = str(doc['production_companies']['origin_country'])
category_1      = str(doc['genres']['name'])
category_2      = str(doc['genres']['name'])
movie_rating    = float(doc['popularity'])
avg_rating      = float(doc['production_companies']['production_countries']['vote_average'])
total_clicks    = float(doc['production_companies']['production_countries']['vote_count'])

# check for nan's in the numeric values and then enter into the database
valid_data  = True
#for valid in np.isnan([lat, lon, humid, press, min_temp, max_temp, temp]):
#	if valid is False:
#		valid_data = False
#		break;

row  =  (id_movie, movie_name, year,  country_origin, category_1, category_2, movie_rating,
        avg_rating, total_clicks)

insert_cmd = """INSERT INTO movies
                (id_movie, movie_name, year,
                country_origin, category_1, category_2,
                movie_rating, avg_rating, total_clicks)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

print(insert_cmd,row)
if valid_data is True:
    pg_hook.run(insert_cmd, parameters=row)
