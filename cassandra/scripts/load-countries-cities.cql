USE weather;

COPY weather.countries(id,name,subregion,region) FROM '/data/countries.csv' WITH HEADER=TRUE;
COPY weather.cities(country_id,city_id,name,latitude,longitude) FROM '/data/cities.csv' WITH HEADER=TRUE;