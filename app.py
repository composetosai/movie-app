from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import boto3
import io
import pandas as pd
import json

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///movie.db'
db = SQLAlchemy(app)

class Movie(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    Title = db.Column(db.String(100), nullable=False)
    Year = db.Column(db.Integer, nullable=False)
    Cast = db.Column(db.String(255), nullable=False)
    Genre = db.Column(db.String(50), nullable=False)

    def __repr__(self):
        return self.Title

def get_csv_data():
    # Add accessandsecret keys here

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket_name = 'movie-vendor-bucket'
    file_list = s3.list_objects_v2(Bucket=bucket_name)
    for obj in file_list['Contents']:
        print(obj['Key'])
        response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
        data = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(data))
        for index, row in df.iterrows():
            # Check if the movie already exists in the database
            existing_movie = Movie.query.filter_by(Title=row['Tittle'], Year=row['Year']).first()
            if not existing_movie:
                # Create and add the movie to the database if it doesn't exist
                movie = Movie(Title=row['Tittle'], Year=row['Year'], Cast=row['Cast'], Genre=row['Genre'])
                db.session.add(movie)
        db.session.commit()

def json_datato_db():
    with open('movies.json','r') as json_file:
        json_data=json.load(json_file)
        
    for row in json_data:
    
        
            # Create and add the movie to the database if it doesn't exist
        try:
            movie = Movie(Title=row['title'], Year=row['year'], Cast=str(row['cast']), Genre=str(row['genres']))
            db.session.add(movie)
            
        except:
            pass
    db.session.commit()

# Endpoint to query movie data
@app.route('/movies', methods=['GET', 'POST'])
def get_movies():
    query_params = {key: value for key, value in request.args.items()}
    filters = {key: query_params[key] for key in ['Year', 'Title', 'Cast', 'Genre'] if key in query_params }
  
    if filters:
        data = Movie.query.filter_by(**filters).all()
        result = [{'Title': movie.Title, 'Year': movie.Year, 'Cast': eval(movie.Cast), 'Genre': eval(movie.Genre)} for movie in data]
    else:
        result = {'error': 'No valid query parameters provided'}
    return jsonify(result)



if __name__ == '__main__':
   with app.app_context():
        db.create_all()
        json_datato_db()
   app.run(debug=True, host="0.0.0.0", port=5000)
